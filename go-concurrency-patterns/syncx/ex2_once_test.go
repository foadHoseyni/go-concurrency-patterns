package syncx

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLazy_LoadsOnce(t *testing.T) {
	var calls atomic.Int32
	l := NewLazy(func() (string, error) {
		calls.Add(1)
		time.Sleep(30 * time.Millisecond)
		return "config-v1", nil
	})

	var wg sync.WaitGroup
	results := make([]string, 100)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := l.Get()
			if err != nil {
				t.Errorf("Get: %v", err)
			}
			results[i] = v
		}()
	}
	wg.Wait()

	if n := calls.Load(); n != 1 {
		t.Errorf("load ran %d times, want exactly 1", n)
	}
	for i, v := range results {
		if v != "config-v1" {
			t.Fatalf("results[%d] = %q, want config-v1", i, v)
		}
	}
}

func TestLazy_RemembersError(t *testing.T) {
	boom := errors.New("disk on fire")
	var calls atomic.Int32
	l := NewLazy(func() (int, error) {
		calls.Add(1)
		return 0, boom
	})
	for i := 0; i < 3; i++ {
		if _, err := l.Get(); !errors.Is(err, boom) {
			t.Errorf("call %d: err = %v, want boom", i, err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("load ran %d times, want 1 (errors are not retried)", n)
	}
}
