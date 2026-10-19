package security

import (
	"testing"
	"time"
)

func TestGenerateUsesUniqueRefreshTokens(t *testing.T) {
	manager := NewJWTManager("test-signing-secret", time.Minute, time.Hour)
	first, err := manager.Generate(7, "user", "alice")
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.Generate(7, "user", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if first.RefreshToken == second.RefreshToken {
		t.Fatal("refresh tokens must differ even when issued in the same second")
	}
	if _, err := manager.Parse(first.RefreshToken, true); err != nil {
		t.Fatalf("generated refresh token did not parse: %v", err)
	}
	if _, err := manager.Parse(first.RefreshToken, false); err == nil {
		t.Fatal("refresh token was accepted as an access token")
	}
}
