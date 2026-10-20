package runtime

import (
	"context"
	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"testing"
	"time"
)

func TestEinoEventsNoTerminalDropWhenConsumerResumes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := newEventSink(1, time.Second, "run", cancel)
	defer sink.Close()
	if err := sink.Send(ctx, agent.EvtDelta("first")); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- sink.Send(ctx, agent.EvtDone("stop")) }()
	first := <-sink.events
	last := <-sink.events
	if first.Seq != 1 || last.Seq != 2 || last.Type != agent.TypeDone || last.RunID != "run" {
		t.Fatalf("terminal/order lost: %+v %+v", first, last)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
