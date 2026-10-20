package einoadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	platformtool "github.com/chengpeng-cp/nexus-agent/internal/tool"
	einotool "github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	einoschema "github.com/eino-contrib/jsonschema"
	validator "github.com/santhosh-tekuri/jsonschema/v6"
)

type ToolSource interface {
	ListContext(context.Context, string) ([]llm.ToolDef, error)
	Invoke(context.Context, string, string, string) platformtool.ToolResult
}

type boundTool struct {
	source      ToolSource
	bindings    string
	definition  llm.ToolDef
	info        *schema.ToolInfo
	validated   *validator.Schema
	fingerprint string
	dangerous   bool
	timeout     time.Duration
	maxOutput   int
}

func BuildTools(ctx context.Context, source ToolSource, bindings string, dangerous map[string]bool, timeout time.Duration, maxOutputBytes int) ([]einotool.BaseTool, error) {
	if source == nil || timeout <= 0 || maxOutputBytes < 64 {
		return nil, fmt.Errorf("tool source, positive timeout and output limit >=64 required")
	}
	defs, err := source.ListContext(ctx, bindings)
	if err != nil {
		return nil, err
	}
	tools := make([]einotool.BaseTool, 0, len(defs))
	seen := map[string]bool{}
	for _, def := range defs {
		if def.Name == "" || seen[def.Name] {
			return nil, fmt.Errorf("invalid or duplicate tool name")
		}
		seen[def.Name] = true
		raw, err := json.Marshal(def.Parameters)
		if err != nil {
			return nil, err
		}
		var document any
		if err = json.Unmarshal(raw, &document); err != nil {
			return nil, err
		}
		if err = checkSchema(document); err != nil {
			return nil, fmt.Errorf("tool %s: %w", def.Name, err)
		}
		compiler := validator.NewCompiler()
		compiler.DefaultDraft(validator.Draft2020)
		compiler.AssertFormat()
		compiler.UseLoader(denySchemaLoader{})
		resource := "https://nexus.invalid/tool-schema"
		if err = compiler.AddResource(resource, document); err != nil {
			return nil, err
		}
		compiled, err := compiler.Compile(resource)
		if err != nil {
			return nil, fmt.Errorf("tool %s schema: %w", def.Name, err)
		}
		var params einoschema.Schema
		if err = json.Unmarshal(raw, &params); err != nil {
			return nil, err
		}
		tools = append(tools, &boundTool{source: source, bindings: bindings, definition: def, info: &schema.ToolInfo{Name: def.Name, Desc: def.Description, ParamsOneOf: schema.NewParamsOneOfByJSONSchema(&params)}, validated: compiled, fingerprint: string(raw), dangerous: def.Dangerous || dangerous[def.Name], timeout: timeout, maxOutput: maxOutputBytes})
	}
	return tools, nil
}

func (t *boundTool) Info(context.Context) (*schema.ToolInfo, error) {
	copy, err := cloneTools([]*schema.ToolInfo{t.info})
	if err != nil {
		return nil, err
	}
	return copy[0], nil
}

func (t *boundTool) InvokableRun(ctx context.Context, args string, _ ...einotool.Option) (string, error) {
	// Refresh authority before invoking: disabled or newly dangerous tools cannot
	// keep executing using metadata captured at the beginning of a long run.
	defs, err := t.source.ListContext(ctx, t.bindings)
	if err != nil {
		return toolObservation("tool_unavailable", "tool permission/configuration unavailable"), nil
	}
	var current *llm.ToolDef
	for i := range defs {
		if defs[i].Name == t.definition.Name {
			current = &defs[i]
			break
		}
	}
	if current == nil {
		return toolObservation("tool_unavailable", "tool is no longer bound"), nil
	}
	if (t.dangerous || current.Dangerous) && !toolApproved(ctx, t.definition.Name, args) {
		return toolObservation("approval_required", "sensitive tool requires persistent approval"), nil
	}
	raw, _ := json.Marshal(current.Parameters)
	if string(raw) != t.fingerprint {
		return toolObservation("tool_configuration_changed", "restart with the updated tool schema"), nil
	}
	if len(args) > 65536 {
		return toolObservation("invalid_arguments", "arguments exceed 64 KiB"), nil
	}
	decoder := json.NewDecoder(strings.NewReader(args))
	decoder.UseNumber()
	var value any
	if err = decoder.Decode(&value); err != nil {
		return toolObservation("invalid_arguments", "arguments must be valid JSON"), nil
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return toolObservation("invalid_arguments", "exactly one JSON value is required"), nil
	}
	if err = t.validated.Validate(value); err != nil {
		return toolObservation("invalid_arguments", err.Error()), nil
	}
	runCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	result := t.source.Invoke(runCtx, t.bindings, t.definition.Name, args)
	if runCtx.Err() != nil {
		return toolObservation("timeout", "tool execution deadline/cancellation reached"), nil
	}
	if result.Status != "ok" {
		return truncateOutput(toolObservation("tool_error", result.Output), t.maxOutput), nil
	}
	return truncateOutput(result.Output, t.maxOutput), nil
}

func toolObservation(code, message string) string {
	raw, _ := json.Marshal(map[string]string{"error": code, "message": message})
	return string(raw)
}
func truncateOutput(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	const marker = "\n[truncated]"
	end := limit - len(marker)
	for end > 0 && !utf8.ValidString(text[:end]) {
		end--
	}
	return text[:end] + marker
}

type denySchemaLoader struct{}

func (denySchemaLoader) Load(string) (any, error) {
	return nil, fmt.Errorf("external schema loading is disabled")
}

// Walk only schema positions: properties named "default" or literal objects in
// enum/default/examples are user data, not additional validation keywords.
func checkSchema(value any) error {
	if _, ok := value.(bool); ok {
		return nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("schema must be an object or boolean")
	}
	allowed := " $schema $id $anchor $dynamicAnchor $ref $dynamicRef $comment $defs definitions type enum const multipleOf maximum exclusiveMaximum minimum exclusiveMinimum maxLength minLength pattern maxItems minItems uniqueItems maxContains minContains maxProperties minProperties required dependentRequired title description default examples deprecated readOnly writeOnly format properties patternProperties dependentSchemas dependencies allOf anyOf oneOf not if then else items prefixItems contains additionalProperties unevaluatedProperties unevaluatedItems propertyNames additionalItems "
	for key, child := range object {
		if !strings.Contains(allowed, " "+key+" ") {
			return fmt.Errorf("unsupported schema keyword %q", key)
		}
		switch key {
		case "$ref", "$dynamicRef":
			ref, ok := child.(string)
			if !ok || !strings.HasPrefix(ref, "#") {
				return fmt.Errorf("only local schema references are allowed")
			}
		case "properties", "patternProperties", "dependentSchemas", "$defs", "definitions":
			if children, ok := child.(map[string]any); ok {
				for _, schema := range children {
					if err := checkSchema(schema); err != nil {
						return err
					}
				}
			}
		case "allOf", "anyOf", "oneOf", "prefixItems":
			if children, ok := child.([]any); ok {
				for _, schema := range children {
					if err := checkSchema(schema); err != nil {
						return err
					}
				}
			}
		case "not", "if", "then", "else", "contains", "additionalProperties", "unevaluatedProperties", "unevaluatedItems", "propertyNames", "additionalItems":
			if err := checkSchema(child); err != nil {
				return err
			}
		case "items":
			if children, ok := child.([]any); ok {
				for _, schema := range children {
					if err := checkSchema(schema); err != nil {
						return err
					}
				}
			} else if err := checkSchema(child); err != nil {
				return err
			}
		case "dependencies":
			if children, ok := child.(map[string]any); ok {
				for _, schema := range children {
					if _, list := schema.([]any); !list {
						if err := checkSchema(schema); err != nil {
							return err
						}
					}
				}
			}
		case "format":
			known := " date-time date time duration email idn-email hostname idn-hostname ipv4 ipv6 uri uri-reference iri iri-reference uuid regex json-pointer relative-json-pointer uri-template "
			name, ok := child.(string)
			if !ok || !strings.Contains(known, " "+name+" ") {
				return fmt.Errorf("unsupported schema format")
			}
		}
	}
	return nil
}
