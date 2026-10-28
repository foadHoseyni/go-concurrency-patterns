package conc

import (
	"errors"
	"runtime"
	"testing"
	"time"
)

func TestWithTimeout_Fast(t *testing.T) {
	v, err := WithTimeout(100*time.Millisecond, func() string { return "ok" })
	if err != nil || v != "ok" {
		t.Errorf("got %q, %v; want ok, nil", v, err)
	}
}

func TestWithTimeout_Slow(t *testing.T) {
	before := runtime.NumGoroutine()
	start := time.Now()
	_, err := WithTimeout(30*time.Millisecond, func() int {
		time.Sleep(100 * time.Millisecond)
		return 1
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
	if e := time.Since(start); e > 80*time.Millisecond {
		t.Errorf("returned after %v; should return right after the 30ms timeout", e)
	}
	// After f finishes (100ms), its goroutine must be able to exit.
	noLeak(t, before)
}
