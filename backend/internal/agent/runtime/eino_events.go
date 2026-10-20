package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
)

var errBackpressure = errors.New("agent event consumer is too slow")
var errSinkClosed = errors.New("agent event stream is closed")

type eventSink struct {
	mu             sync.Mutex
	events         chan agent.Event
	timeout        time.Duration
	runID          string
	seq            uint64
	closed, failed bool
	cancel         context.CancelFunc
}

func newEventSink(buffer int, timeout time.Duration, runID string, cancel context.CancelFunc) *eventSink {
	return &eventSink{events: make(chan agent.Event, buffer), timeout: timeout, runID: runID, cancel: cancel}
}
func (s *eventSink) Send(ctx context.Context, ev agent.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errSinkClosed
	}
	if s.failed {
		return errBackpressure
	}
	ev.RunID = s.runID
	ev.Seq = s.seq + 1
	ev.At = time.Now().UTC()
	timer := time.NewTimer(s.timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case s.events <- ev:
		s.seq++
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		s.failed = true
		s.cancel()
		return errBackpressure
	}
}
func (s *eventSink) Failed() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.failed }
func (s *eventSink) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
}
