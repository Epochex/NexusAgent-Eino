// 用户数据访问。
package repo

import (
	"context"
	"errors"

	"gorm.io/gorm"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
)

var ErrUserAlreadyExists = errors.New("user already exists")

// UserRepo 用户表。
type UserRepo struct {
	db *gorm.DB
}

// Create 创建用户。
func (r *UserRepo) Create(ctx context.Context, u *model.User) error {
	return r.db.WithContext(ctx).Create(u).Error
}

// CreateFirstOrUser serializes account creation so only one concurrent first
// registration can receive the admin role.
func (r *UserRepo) CreateFirstOrUser(ctx context.Context, u *model.User) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("LOCK TABLE users IN EXCLUSIVE MODE").Error; err != nil {
			return err
		}
		var count int64
		if err := tx.Model(&model.User{}).Count(&count).Error; err != nil {
			return err
		}
		var exists int64
		if err := tx.Model(&model.User{}).
			Where("username = ? OR email = ?", u.Username, u.Email).
			Count(&exists).Error; err != nil {
			return err
		}
		if exists > 0 {
			return ErrUserAlreadyExists
		}
		if count == 0 {
			u.Role = "admin"
		} else {
			u.Role = "user"
		}
		return tx.Create(u).Error
	})
}

// GetByID 按 ID 查询。
func (r *UserRepo) GetByID(ctx context.Context, id int64) (*model.User, error) {
	var u model.User
	if err := r.db.WithContext(ctx).First(&u, id).Error; err != nil {
		return nil, err
	}
	return &u, nil
}

// GetByUsername 按用户名查询（登录）。
func (r *UserRepo) GetByUsername(ctx context.Context, username string) (*model.User, error) {
	var u model.User
	if err := r.db.WithContext(ctx).Where("username = ?", username).First(&u).Error; err != nil {
		return nil, err
	}
	return &u, nil
}

// ExistsBy 徽记查询：用户名/邮箱是否已存在。
func (r *UserRepo) ExistsBy(ctx context.Context, username, email string) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&model.User{}).
		Where("username = ? OR email = ?", username, email).
		Count(&count).Error
	return count > 0, err
}

// Update 更新用户。
func (r *UserRepo) Update(ctx context.Context, u *model.User) error {
	return r.db.WithContext(ctx).Save(u).Error
}

// Count 统计用户总数（首个注册用户提升为 admin）。
func (r *UserRepo) Count(ctx context.Context) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&model.User{}).Count(&n).Error
	return n, err
}
