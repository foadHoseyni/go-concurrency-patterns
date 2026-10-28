package syncx

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

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

// maxTracker records the highest number of things running at the same time.
type maxTracker struct{ cur, max atomic.Int32 }

func (m *maxTracker) enter() {
	n := m.cur.Add(1)
	for {
		old := m.max.Load()
		if n <= old || m.max.CompareAndSwap(old, n) {
			return
		}
	}
}
func (m *maxTracker) leave() { m.cur.Add(-1) }

// within fails the test if f doesn't return within d.
func within(t *testing.T, d time.Duration, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %v (blocked?)", what, d)
	}
}
