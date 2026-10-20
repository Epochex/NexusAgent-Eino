package config

import "testing"

func TestAgentRuntimeLimitsDefaultAndRejectInvalid(t *testing.T) {
	cfg := &Config{Env: "dev"}
	if err := validate(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Agent.EventBuffer != 128 || cfg.Agent.InputTokenBudget != 128000 || cfg.Agent.MaxConcurrentTools != 4 {
		t.Fatalf("runtime defaults missing: %+v", cfg.Agent)
	}
	cfg.Agent.EventBuffer = -1
	if err := validate(cfg); err == nil {
		t.Fatal("negative event buffer accepted")
	}
}

func TestValidateProductionSecrets(t *testing.T) {
	cfg := &Config{
		Env:       "prod",
		JWT:       JWT{Secret: "change-me-in-prod"},
		SecretKey: "change-me-32-bytes!!",
	}
	if err := validate(cfg); err == nil {
		t.Fatal("production accepted default secrets")
	}

	cfg.JWT.Secret = "jwt-secret-with-at-least-32-characters"
	cfg.SecretKey = "encryption-secret-with-at-least-32-chars"
	if err := validate(cfg); err != nil {
		t.Fatalf("production rejected configured secrets: %v", err)
	}
}

func TestValidateAllowsDevelopmentDefaults(t *testing.T) {
	cfg := &Config{Env: "dev", JWT: JWT{Secret: "change-me-in-prod"}, SecretKey: "change-me-32-bytes!!"}
	if err := validate(cfg); err != nil {
		t.Fatalf("development defaults should remain usable: %v", err)
	}
}

func TestValidateRejectsReusedProductionSecrets(t *testing.T) {
	secret := "same-secret-with-at-least-32-characters"
	cfg := &Config{Env: "production", JWT: JWT{Secret: secret}, SecretKey: secret}
	if err := validate(cfg); err == nil {
		t.Fatal("production accepted a shared JWT and encryption secret")
	}
}
