// Package callmeta carries control-plane call identity across protocol adapters.
package callmeta

import "context"

type operationKey struct{}

func WithOperationID(ctx context.Context, id string) context.Context {
	if len(id) == 0 || len(id) > 64 {
		return ctx
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
			return ctx
		}
	}
	return context.WithValue(ctx, operationKey{}, id)
}
func OperationID(ctx context.Context) string { id, _ := ctx.Value(operationKey{}).(string); return id }
