package syncx

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProcessor_FinishesInFlightWork(t *testing.T) {
	before := runtime.NumGoroutine()
	var handled atomic.Int32
	p := NewProcessor(2, func(ctx context.Context, job int) {
		time.Sleep(30 * time.Millisecond)
		handled.Add(1)
	})
	for i := 0; i < 5; i++ {
		if err := p.Submit(i); err != nil {
			t.Fatalf("Submit(%d): %v", i, err)
		}
	}

	var err error
	within(t, 2*time.Second, "Shutdown", func() { err = p.Shutdown(context.Background()) })
	if err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	if n := handled.Load(); n != 5 {
		t.Errorf("handled %d jobs, want all 5 accepted jobs before Shutdown returns", n)
	}
	if err := p.Submit(99); !errors.Is(err, ErrStopped) {
		t.Errorf("Submit after Shutdown: err = %v, want ErrStopped", err)
	}
	noLeak(t, before)
}

func TestProcessor_ShutdownTimeout(t *testing.T) {
	var sawCancel atomic.Int32
	p := NewProcessor(1, func(ctx context.Context, job int) {
		select {
		case <-ctx.Done():
			sawCancel.Add(1)
		case <-time.After(2 * time.Second):
		}
	})
	if err := p.Submit(1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond) // let the worker pick it up

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := p.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	if e := time.Since(start); e > 300*time.Millisecond {
		t.Errorf("Shutdown took %v; it must give up when ctx expires", e)
	}
	deadline := time.Now().Add(time.Second)
	for sawCancel.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if sawCancel.Load() != 1 {
		t.Error("the running handler's context was not cancelled after the timeout")
	}
}

// Run with -race: Submit racing with Shutdown must never panic.
func TestProcessor_SubmitDuringShutdown(t *testing.T) {
	p := NewProcessor(4, func(ctx context.Context, job int) {})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if err := p.Submit(j); err != nil && !errors.Is(err, ErrStopped) {
					t.Errorf("Submit: unexpected error %v", err)
					return
				}
			}
		}()
	}
	time.Sleep(time.Millisecond)
	within(t, 2*time.Second, "Shutdown", func() { p.Shutdown(context.Background()) })
	wg.Wait()
}
