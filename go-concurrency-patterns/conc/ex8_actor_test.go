package conc

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestTracker_Basic(t *testing.T) {
	tr := NewTracker(map[string]int{"T1": 5})
	defer tr.Close()

	if err := tr.Load("T1", 3); err != nil {
		t.Fatal(err)
	}
	if err := tr.Load("T1", 3); !errors.Is(err, ErrFull) {
		t.Errorf("err = %v, want ErrFull", err)
	}
	if free, err := tr.Free("T1"); err != nil || free != 2 {
		t.Errorf("free = %d, %v; want 2, nil (the failed load must not change anything)", free, err)
	}
	if err := tr.Load("T9", 1); !errors.Is(err, ErrUnknownTruck) {
		t.Errorf("err = %v, want ErrUnknownTruck", err)
	}
}

// Run with -race.
func TestTracker_ConcurrentLoadsNeverOverfill(t *testing.T) {
	tr := NewTracker(map[string]int{"T1": 50})
	defer tr.Close()

	var ok atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tr.Load("T1", 1) == nil {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()

	if n := ok.Load(); n != 50 {
		t.Errorf("%d loads succeeded, want exactly 50", n)
	}
	if free, _ := tr.Free("T1"); free != 0 {
		t.Errorf("free = %d, want 0", free)
	}
}

func TestTracker_Close(t *testing.T) {
	before := runtime.NumGoroutine()
	tr := NewTracker(map[string]int{"T1": 5})
	tr.Close()
	tr.Close() // second Close must not panic

	if err := tr.Load("T1", 1); !errors.Is(err, ErrClosed) {
		t.Errorf("Load after Close: err = %v, want ErrClosed", err)
	}
	if _, err := tr.Free("T1"); !errors.Is(err, ErrClosed) {
		t.Errorf("Free after Close: err = %v, want ErrClosed", err)
	}
	noLeak(t, before)
}
