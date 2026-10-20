// 认证服务：注册 / 登录 / 刷新 / 登出。
package service

import (
	"context"
	"errors"
	"net/mail"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

var rotateRefreshTokenScript = redis.NewScript(`
if redis.call('SREM', KEYS[1], ARGV[1]) == 0 then
	return 0
end
redis.call('SADD', KEYS[1], ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`)

// AuthService 认证业务。
type AuthService struct {
	repos *repo.Repos
	users *repo.UserRepo
	jwt   *security.JWTManager
	rdb   *redis.Client
}

// NewAuthService 构造。
func NewAuthService(repos *repo.Repos, jwt *security.JWTManager, rdb *redis.Client) *AuthService {
	return &AuthService{repos: repos, users: repos.User, jwt: jwt, rdb: rdb}
}

// RegisterInput 注册参数。
type RegisterInput struct {
	Username string `json:"username" vd:"len($)>2"`
	Email    string `json:"email"`
	Password string `json:"password" vd:"len($)>7"`
}

// Register 注册（首个用户自动成为 admin，方便开箱即用）。
func (s *AuthService) Register(ctx context.Context, in *RegisterInput) (*model.User, error) {
	in.Username = strings.TrimSpace(in.Username)
	in.Email = strings.TrimSpace(in.Email)
	if len(in.Username) < 3 || len(in.Username) > 64 {
		return nil, errcode.ErrInvalidParam.WithMsg("用户名长度必须为 3 至 64 个字符")
	}
	if addr, err := mail.ParseAddress(in.Email); err != nil || addr.Address != in.Email || len(in.Email) > 128 {
		return nil, errcode.ErrInvalidParam.WithMsg("请提供有效邮箱地址")
	}
	if len(in.Password) < 12 || len(in.Password) > 72 {
		return nil, errcode.ErrInvalidParam.WithMsg("密码长度必须为 12 至 72 个字节")
	}
	if exists, err := s.users.ExistsBy(ctx, in.Username, in.Email); err != nil {
		return nil, errcode.ErrInternal.WithCause(err)
	} else if exists {
		return nil, errcode.ErrUserExists
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, errcode.ErrInternal.WithCause(err)
	}

	u := &model.User{Username: in.Username, Email: in.Email, PasswordHash: string(hash)}
	if err := s.users.CreateFirstOrUser(ctx, u); err != nil {
		if errors.Is(err, repo.ErrUserAlreadyExists) {
			return nil, errcode.ErrUserExists
		}
		return nil, errcode.ErrInternal.WithCause(err)
	}
	return u, nil
}

// Login 登录：校验密码 → 签发双 Token。
func (s *AuthService) Login(ctx context.Context, username, password string) (*security.TokenPair, error) {
	u, err := s.users.GetByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errcode.ErrLoginFailed
		}
		return nil, errcode.ErrInternal.WithCause(err)
	}
	if u.Status != 1 {
		return nil, errcode.ErrUserLocked
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)); err != nil {
		return nil, errcode.ErrLoginFailed
	}
	pair, err := s.jwt.Generate(u.ID, u.Role, u.Username)
	if err != nil {
		return nil, errcode.ErrInternal.WithCause(err)
	}
	if err := s.StoreRefreshToken(ctx, pair.UserID, pair.RefreshToken, s.jwt.RefreshTTL()); err != nil {
		return nil, errcode.ErrInternal.WithCause(err)
	}
	return pair, nil
}

// Refresh 刷新 Access Token（Refresh Token 白名单校验）。
func (s *AuthService) Refresh(ctx context.Context, refreshToken string) (*security.TokenPair, error) {
	claims, err := s.jwt.Parse(refreshToken, true)
	if err != nil {
		// 过期与无效统一按"凭证失效"处理（前端引导重新登录）
		return nil, errcode.ErrTokenInvalid
	}
	principal, err := s.Identity(ctx, claims.UserID)
	if err != nil {
		return nil, err
	}
	pair, err := s.jwt.Generate(principal.ID, principal.Role, principal.Username)
	if err != nil {
		return nil, errcode.ErrInternal.WithCause(err)
	}
	rotated, err := rotateRefreshTokenScript.Run(ctx, s.rdb,
		[]string{refreshKey(claims.UserID)}, refreshToken, pair.RefreshToken,
		s.jwt.RefreshTTL().Milliseconds()).Int()
	if err != nil {
		return nil, errcode.ErrInternal.WithCause(err)
	}
	if rotated != 1 {
		return nil, errcode.ErrTokenInvalid.WithMsg("refresh token 已失效")
	}
	return pair, nil
}

// Identity rechecks current account authority; signed claims alone can outlive
// a role change or account disable. Database failures fail closed.
func (s *AuthService) Identity(parent context.Context, id int64) (*model.User, error) {
	if id < 1 {
		return nil, errcode.ErrUnauthorized
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	user, err := s.users.GetByID(ctx, id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, errcode.ErrUnauthorized
	}
	if err != nil {
		return nil, errcode.ErrInternal.WithCause(err)
	}
	if user.Status != 1 {
		return nil, errcode.ErrUnauthorized
	}
	return user, nil
}

// Logout 登出：吊销该用户全部 refresh token。
func (s *AuthService) Logout(ctx context.Context, userID int64) error {
	return s.rdb.Del(ctx, refreshKey(userID)).Err()
}

// StoreRefreshToken 签发后把 refresh token 加入白名单（TTL 与 refresh_ttl 一致）。
func (s *AuthService) StoreRefreshToken(ctx context.Context, userID int64, token string, ttl time.Duration) error {
	key := refreshKey(userID)
	pipe := s.rdb.Pipeline()
	pipe.SAdd(ctx, key, token)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func refreshKey(userID int64) string {
	return "nx:auth:refresh:" + strconv.FormatInt(userID, 10)
}
