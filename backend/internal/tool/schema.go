package tool

import (
	"fmt"
	validator "github.com/santhosh-tekuri/jsonschema/v6"
)

type denyToolSchemaLoader struct{}

func (denyToolSchemaLoader) Load(string) (any, error) {
	return nil, fmt.Errorf("external tool schema references are disabled")
}
func validateToolArguments(parameters map[string]any, args map[string]any) error {
	compiler := validator.NewCompiler()
	compiler.DefaultDraft(validator.Draft2020)
	compiler.UseLoader(denyToolSchemaLoader{})
	const name = "https://nexus.invalid/executor-tool-schema"
	if err := compiler.AddResource(name, parameters); err != nil {
		return err
	}
	compiled, err := compiler.Compile(name)
	if err != nil {
		return err
	}
	return compiled.Validate(args)
}
