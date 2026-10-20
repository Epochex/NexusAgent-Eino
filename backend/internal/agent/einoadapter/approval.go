package einoadapter

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"strings"
)

type approvalKey struct{}
type exactApproval struct {
	code   string
	digest [32]byte
}

// WithToolApproval is an internal control-plane capability. Only the durable
// driver supplies it after verifying the persisted owner/interrupt/argument hash.
// It is not accepted from request headers, JSON, model output or ordinary Agent input.
func WithToolApproval(ctx context.Context, code, args string) context.Context {
	digest, ok := argumentDigest(args)
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, approvalKey{}, exactApproval{code: code, digest: digest})
}
func argumentDigest(args string) ([32]byte, bool) {
	var value any
	d := json.NewDecoder(strings.NewReader(args))
	d.UseNumber()
	if len(args) > 65536 || d.Decode(&value) != nil || d.Decode(new(any)) != io.EOF {
		return [32]byte{}, false
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return [32]byte{}, false
	}
	return sha256.Sum256(raw), true
}
func toolApproved(ctx context.Context, code, args string) bool {
	capability, ok := ctx.Value(approvalKey{}).(exactApproval)
	if !ok || capability.code != code {
		return false
	}
	digest, valid := argumentDigest(args)
	return valid && digest == capability.digest
}
