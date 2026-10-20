package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/mcp"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type mcpCredentialEnvelope struct {
	Headers map[string]string          `json:"headers"`
	Legacy  map[string]json.RawMessage `json:"legacy,omitempty"`
}

func (s *MCPService) credentials(srv *model.MCPServer) (mcpCredentialEnvelope, error) {
	envelope := mcpCredentialEnvelope{Headers: map[string]string{}}
	if srv.EncryptedCredentials != "" {
		if s.svcs.Encryptor == nil {
			return envelope, fmt.Errorf("MCP credential encryption unavailable")
		}
		raw, err := s.svcs.Encryptor.Decrypt(srv.EncryptedCredentials)
		if err != nil {
			return envelope, fmt.Errorf("MCP credentials cannot be decrypted")
		}
		if len(raw) > 65536 || json.Unmarshal([]byte(raw), &envelope) != nil {
			return envelope, fmt.Errorf("invalid encrypted MCP credentials")
		}
	} else {
		if len(srv.Headers) > 16384 {
			return envelope, fmt.Errorf("legacy MCP headers exceed limit")
		}
		if len(srv.Headers) > 0 && json.Unmarshal(srv.Headers, &envelope.Headers) != nil {
			return envelope, fmt.Errorf("invalid legacy MCP headers")
		}
		legacy := map[string]json.RawMessage{}
		if len(srv.Env) > 0 && string(srv.Env) != "{}" {
			legacy["env"] = append([]byte(nil), srv.Env...)
		}
		if len(srv.Args) > 0 && string(srv.Args) != "[]" {
			legacy["args"] = append([]byte(nil), srv.Args...)
		}
		if srv.Command != "" {
			encoded, _ := json.Marshal(srv.Command)
			legacy["command"] = encoded
		}
		if len(legacy) > 0 {
			envelope.Legacy = legacy
		}
	}
	if envelope.Headers == nil {
		envelope.Headers = map[string]string{}
	}
	if err := mcp.ValidateHeaders(envelope.Headers); err != nil {
		return envelope, err
	}
	return envelope, nil
}
func (s *MCPService) seal(srv *model.MCPServer, envelope mcpCredentialEnvelope) error {
	if s.svcs.Encryptor == nil {
		return fmt.Errorf("MCP credential encryption unavailable")
	}
	if err := mcp.ValidateHeaders(envelope.Headers); err != nil {
		return err
	}
	raw, err := json.Marshal(envelope)
	if err != nil || len(raw) > 65536 {
		return fmt.Errorf("MCP credential envelope exceeds limit")
	}
	cipher, err := s.svcs.Encryptor.Encrypt(string(raw))
	if err != nil {
		return err
	}
	srv.EncryptedCredentials = cipher
	srv.HeadersConfigured = len(envelope.Headers) > 0
	srv.Headers = datatypes.JSON(`{}`)
	srv.Env = datatypes.JSON(`{}`)
	srv.Args = datatypes.JSON(`[]`)
	srv.Command = ""
	return nil
}
func redactMCP(srv *model.MCPServer) *model.MCPServer {
	clone := *srv
	clone.CredentialsPendingMigration = (len(srv.Headers) > 0 && string(srv.Headers) != "{}") || (len(srv.Env) > 0 && string(srv.Env) != "{}") || (len(srv.Args) > 0 && string(srv.Args) != "[]") || srv.Command != ""
	clone.Headers = nil
	clone.Env = nil
	clone.Args = nil
	clone.Command = ""
	clone.EncryptedCredentials = ""
	clone.ClearHeaders = false
	return &clone
}

// MigrateCredentials is an explicit administrator operation, also used by sync.
// It changes representation only; endpoint/authority versions are retained.
func (s *MCPService) MigrateCredentials(ctx context.Context, id int64) error {
	if _, err := s.admin(ctx); err != nil {
		return err
	}
	return s.transaction(ctx, func(tx *gorm.DB) error {
		srv, err := lockedMCP(tx, id)
		if err != nil {
			return err
		}
		envelope, err := s.credentials(srv)
		if err != nil {
			return err
		}
		if err = s.seal(srv, envelope); err != nil {
			return err
		}
		return tx.Model(srv).Updates(map[string]any{"encrypted_credentials": srv.EncryptedCredentials, "headers_present": srv.HeadersConfigured, "headers": datatypes.JSON(`{}`), "env": datatypes.JSON(`{}`), "args": datatypes.JSON(`[]`), "command": ""}).Error
	})
}
