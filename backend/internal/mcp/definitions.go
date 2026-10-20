package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	validator "github.com/santhosh-tekuri/jsonschema/v6"
	"math"
	"strings"
	"unicode/utf8"
)

func ValidToolName(name string) bool {
	if len(name) < 1 || len(name) > 128 {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}

type denySchemaLoader struct{}

func (denySchemaLoader) Load(string) (any, error) {
	return nil, fmt.Errorf("external MCP schema references are disabled")
}
func convertTool(remote *sdk.Tool) (ToolDef, error) {
	if remote == nil || !ValidToolName(remote.Name) || len(remote.Description) > 4096 || !utf8.ValidString(remote.Description) {
		return ToolDef{}, fmt.Errorf("invalid MCP tool identity or description")
	}
	raw, err := json.Marshal(remote.InputSchema)
	if err != nil || len(raw) > 16384 {
		return ToolDef{}, fmt.Errorf("MCP input schema exceeds 16KiB")
	}
	var schema map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&schema) != nil || schema == nil || schema["type"] != "object" {
		return ToolDef{}, fmt.Errorf("MCP tool requires an object input schema")
	}
	headers := map[string]bool{}
	var walk func(any, int) error
	walk = func(value any, depth int) error {
		if depth > 32 {
			return fmt.Errorf("MCP input schema nesting exceeds 32")
		}
		switch current := value.(type) {
		case map[string]any:
			for _, key := range []string{"$ref", "$dynamicRef"} {
				if reference, ok := current[key]; ok {
					ref, ok := reference.(string)
					if !ok || !strings.HasPrefix(ref, "#") {
						return fmt.Errorf("external MCP schema references are disabled")
					}
				}
			}
			if annotation, ok := current["x-mcp-header"]; ok {
				name, ok := annotation.(string)
				if !ok || !validHeaderName(name) || headers[strings.ToLower(name)] {
					return fmt.Errorf("invalid or duplicate MCP parameter header")
				}
				kind, _ := current["type"].(string)
				if kind != "string" && kind != "integer" && kind != "boolean" {
					return fmt.Errorf("MCP parameter headers require scalar properties")
				}
				headers[strings.ToLower(name)] = true
			}
			for _, nested := range current {
				if err := walk(nested, depth+1); err != nil {
					return err
				}
			}
		case []any:
			for _, nested := range current {
				if err := walk(nested, depth+1); err != nil {
					return err
				}
			}
		case json.Number:
			if number, err := current.Float64(); err != nil || math.IsInf(number, 0) || math.IsNaN(number) || math.Trunc(number) == number && math.Abs(number) > 9007199254740991 {
				return fmt.Errorf("MCP schema number exceeds exact protocol representation")
			}
		}
		return nil
	}
	if err = walk(schema, 0); err != nil {
		return ToolDef{}, err
	}
	compiler := validator.NewCompiler()
	compiler.DefaultDraft(validator.Draft2020)
	compiler.UseLoader(denySchemaLoader{})
	const resource = "https://nexus.invalid/mcp-tool-schema"
	if err = compiler.AddResource(resource, schema); err != nil {
		return ToolDef{}, err
	}
	compiled, err := compiler.Compile(resource)
	if err != nil {
		return ToolDef{}, fmt.Errorf("invalid MCP input schema: %w", err)
	}
	return ToolDef{Name: remote.Name, Description: remote.Description, InputSchema: schema, validate: compiled.Validate}, nil
}
