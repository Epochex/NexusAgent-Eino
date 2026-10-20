package tool

import "context"

type bindingKey struct{}
type bindingIdentity struct {
	name    string
	version int64
}

// The runtime binds its exact tool definition to the final invocation. This
// value is process-local authority and is never accepted from request JSON.
func WithBindingVersion(ctx context.Context, name string, version int64) context.Context {
	return context.WithValue(ctx, bindingKey{}, bindingIdentity{name, version})
}
func ExpectedBindingVersion(ctx context.Context, name string) (int64, bool) {
	identity, ok := ctx.Value(bindingKey{}).(bindingIdentity)
	if !ok {
		return 0, false
	}
	if identity.name != name {
		return -1, true
	}
	return identity.version, true
}
