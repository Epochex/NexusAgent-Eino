# SSE cancellation and EOF shutdown race

## Observed failure

The [feature push run](https://github.com/Epochex/NexusAgent-Eino/actions/runs/41218023705)
failed `TestChatServerProcessShutdownDrainsActiveAndIdleStreams`: after SIGTERM,
the process did not exit within the existing four-second test bound. The
[same revision's PR run](https://github.com/Epochex/NexusAgent-Eino/actions/runs/41218039651)
passed all four jobs. Merging alone cannot explain or fix this timing-dependent behavior.

## Mechanism and reproduction

Chat and workflow SSE share `sseBody`. On cancellation, the producer can close
its pipe before the asynchronous `context.AfterFunc` callback runs. The reader
then observes ordinary EOF with `aborted=false`. The response completes as a
normal chunked body and the socket can return to HTTP keep-alive idle, while
the standard transport still counts that connection as active during shutdown.

Returning a context error by itself is insufficient: installed Hertz v0.10.6
`http1/ext.WriteBodyChunked` logs reader errors, writes a final empty chunk and
clears the error. It must also encounter a failed write/flush to leave the
connection loop.

The new actual TCP regression deliberately leaves the async abort callback
unscheduled, closes the producer after context cancellation, and leaves the
client idle pool intact until after server shutdown. With the original reader,
the 500ms shutdown context expires. A context-error-only change also fails.
The successful correction expires the write deadline synchronously as well.

## Correction

Both Chat and workflow SSE constructors retain their `streamCtx` in `sseBody`.
After each pipe read, the reader checks context cancellation under the existing
mutex, records aborted state and immediately expires the socket write deadline.
Hertz's subsequent chunk flush fails and Hertz closes the connection using its
own lifecycle. The application does not call `Conn.Close`, avoiding concurrent
cleanup of the framework's connection-state watcher.

An uncanceled EOF keeps its existing normal-completion behavior. The process
SIGTERM deadline and the framework's transport shutdown limits are unchanged.

## Evidence and scope

- The actual TCP regression failed on the original behavior and on the
  context-error-only intermediate correction.
- The complete correction passed 20 consecutive runs under `go test -race`.
- Independent review reproduced the failing intermediate state and passed the
  final correction three times. Its test cleanup recommendation was applied.
- After that cleanup, the complete API package passed `go test -race
  ./internal/api -count=1 -timeout=120s`; the full backend passed `go test ./...
  -p=4 -count=1 -timeout=120s` and `go vet ./...`.
- Tests: `backend/internal/api/sse_cancel_test.go`; reader and Chat wiring:
  `backend/internal/api/handler_chat.go`; workflow wiring:
  `backend/internal/api/handler_workflow_runtime.go`.

This establishes the deterministic cancellation/EOF window and its correction;
it is not a claim that every possible process-exit timeout or CI failure shares
the same cause. Full backend and GitHub process/infrastructure checks must be
reported separately for each published revision.
