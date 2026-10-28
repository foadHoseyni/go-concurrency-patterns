package conc

import (
	"runtime"
	"testing"
	"time"
)

// collect reads everything from ch until it is closed. If the channel is not
// closed within the timeout, the test fails instead of hanging forever.
func collect[T any](t *testing.T, ch <-chan T, timeout time.Duration) []T {
	t.Helper()
	var out []T
	deadline := time.After(timeout)
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, v)
		case <-deadline:
			t.Fatalf("channel not closed after %v (got %d values so far) - did you forget close()?", timeout, len(out))
			return nil
		}
	}
}

// noLeak fails if, within one second, the number of goroutines doesn't drop
// back to what it was before the test started.
func noLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("goroutine leak: %d running, expected at most %d", runtime.NumGoroutine(), before)
}
