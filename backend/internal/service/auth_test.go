package service

import (
	"context"
	"testing"
)

func TestRegisterRejectsWeakOrInvalidInput(t *testing.T) {
	service := &AuthService{}
	cases := []RegisterInput{
		{Username: "ab", Email: "user@example.com", Password: "long-enough-password"},
		{Username: "alice", Email: "not-an-email", Password: "long-enough-password"},
		{Username: "alice", Email: "alice@example.com", Password: "short"},
	}
	for _, input := range cases {
		if _, err := service.Register(context.Background(), &input); err == nil {
			t.Errorf("Register(%+v) unexpectedly accepted invalid input", input)
		}
	}
}
