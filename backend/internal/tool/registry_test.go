package tool

import (
	"context"
	"testing"
)

func TestRegistryPrefixDispatchIsDynamicAndDoesNotHoldLock(t *testing.T) {
	registry := NewRegistry()
	dynamic, ok := registry.(DynamicRegistry)
	if !ok {
		t.Fatal("registry lacks dynamic dispatch")
	}
	handler := func(context.Context, map[string]any, string) (string, error) { return "dynamic", nil }
	dynamic.RegisterPrefix("mcp_", func(code string) (BuiltinHandler, bool) { registry.Register("memo", handler); return handler, true })
	if _, ok = registry.Get("other"); ok {
		t.Fatal("prefix matched another namespace")
	}
	call, ok := registry.Get("mcp_added_after_startup")
	if !ok {
		t.Fatal("new remote tool missing after startup")
	}
	if value, err := call(context.Background(), nil, ""); err != nil || value != "dynamic" {
		t.Fatal(value, err)
	}
	registry.Register("mcp_fixed", func(context.Context, map[string]any, string) (string, error) { return "explicit", nil })
	call, _ = registry.Get("mcp_fixed")
	if value, _ := call(context.Background(), nil, ""); value != "explicit" {
		t.Fatal("explicit handler lost priority")
	}
	dynamic.Unregister("mcp_fixed")
	call, _ = registry.Get("mcp_fixed")
	if value, _ := call(context.Background(), nil, ""); value != "dynamic" {
		t.Fatal("revoked cached handler still returned")
	}
	if len(registry.Codes()) != 1 {
		t.Fatal("prefix created unbounded registry entries", registry.Codes())
	}
}
