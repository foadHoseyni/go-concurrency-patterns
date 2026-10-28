package syncx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSemaphore_Limits(t *testing.T) {
	sem := NewSemaphore(3)
	var m maxTracker
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sem.Acquire(context.Background()); err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			m.enter()
			time.Sleep(10 * time.Millisecond)
			m.leave()
			sem.Release()
		}()
	}
	wg.Wait()
	if got := m.max.Load(); got != 3 {
		t.Errorf("max concurrent = %d, want exactly 3", got)
	}
}

func TestSemaphore_CancelWhileWaiting(t *testing.T) {
	sem := NewSemaphore(1)
	if err := sem.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var err error
	within(t, time.Second, "Acquire on a full semaphore with a 30ms deadline", func() {
		err = sem.Acquire(ctx)
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}

	// The failed Acquire must not have taken a slot: after one Release,
	// exactly one Acquire succeeds immediately.
	sem.Release()
	within(t, 100*time.Millisecond, "Acquire after Release", func() {
		if err := sem.Acquire(context.Background()); err != nil {
			t.Errorf("Acquire after Release: %v", err)
		}
	})
}
