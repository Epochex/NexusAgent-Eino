// Package a2a uses official protocol types on the wire. Task below is only the
// platform's normalized remote-delegation result and is never a JSON-RPC payload.
package a2a

import sdk "github.com/a2aproject/a2a-go/v2/a2a"

type AgentCard = sdk.AgentCard
type AgentSkill = sdk.AgentSkill
type Part struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}
type Artifact struct {
	Name  string `json:"name"`
	Parts []Part `json:"parts"`
}
type Task struct {
	ID        string     `json:"id"`
	State     string     `json:"state"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
}
