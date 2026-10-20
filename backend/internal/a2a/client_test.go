package a2a

import (
	"context"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

type protocolExecutor struct{}

func (protocolExecutor) Execute(_ context.Context, ctx *a2asrv.ExecutorContext) iter.Seq2[sdk.Event, error] {
	return func(yield func(sdk.Event, error) bool) {
		if !yield(sdk.NewSubmittedTask(ctx, ctx.Message), nil) {
			return
		}
		if !yield(sdk.NewArtifactEvent(ctx, sdk.NewTextPart("official protocol response")), nil) {
			return
		}
		yield(sdk.NewStatusUpdateEvent(ctx, sdk.TaskStateCompleted, nil), nil)
	}
}
func (protocolExecutor) Cancel(_ context.Context, ctx *a2asrv.ExecutorContext) iter.Seq2[sdk.Event, error] {
	return func(yield func(sdk.Event, error) bool) {
		yield(sdk.NewStatusUpdateEvent(ctx, sdk.TaskStateCanceled, nil), nil)
	}
}
func TestA2AClientOfficialCurrentProtocol(t *testing.T) {
	for _, version := range []sdk.ProtocolVersion{"1.0", "1.0.1"} {
		t.Run(string(version), func(t *testing.T) { testA2ACurrentProtocol(t, version) })
	}
}
func testA2ACurrentProtocol(t *testing.T, version sdk.ProtocolVersion) {
	mux := http.NewServeMux()
	peer := httptest.NewServer(mux)
	defer peer.Close()
	card := &sdk.AgentCard{Name: "current-agent", Version: "1", SupportedInterfaces: []*sdk.AgentInterface{sdk.NewAgentInterface(peer.URL+"/a2a", sdk.TransportProtocolJSONRPC)}}
	card.SupportedInterfaces[0].ProtocolVersion = version
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	mux.Handle("/a2a", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(protocolExecutor{})))
	client := NewClient(WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: []string{peer.URL}}))
	defer client.Close()
	found, err := client.FetchCard(context.Background(), peer.URL)
	if err != nil || found.Name != "current-agent" {
		t.Fatal("current official AgentCard discovery failed", found, err)
	}
	task, err := client.SendMessage(context.Background(), peer.URL, "Inspect")
	if err != nil || task.State != "completed" || !strings.Contains(ExtractText(task), "official protocol response") {
		t.Fatal("official task result lost", task, err)
	}
	queried, err := client.GetTask(context.Background(), peer.URL, found, task.ID)
	if err != nil || queried.ID != task.ID || queried.State != "completed" {
		t.Fatal("official tasks/get failed", queried, err)
	}
}
