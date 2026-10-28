package syncx

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroup_Deduplicates(t *testing.T) {
	var g Group[string]
	var calls atomic.Int32
	fn := func() (string, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return "page", nil
	}

	var wg sync.WaitGroup
	var sharedCount atomic.Int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err, shared := g.Do("product-42", fn)
			if err != nil || v != "page" {
				t.Errorf("got %q, %v", v, err)
			}
			if shared {
				sharedCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if n := calls.Load(); n != 1 {
		t.Errorf("fn ran %d times, want 1", n)
	}
	if n := sharedCount.Load(); n != 10 {
		t.Errorf("%d callers got shared=true, want 10", n)
	}
}

func TestGroup_ErrorIsShared(t *testing.T) {
	var g Group[int]
	boom := errors.New("db down")
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err, _ := g.Do("k", func() (int, error) {
				time.Sleep(30 * time.Millisecond)
				return 0, boom
			})
			if !errors.Is(err, boom) {
				t.Errorf("err = %v, want boom", err)
			}
		}()
	}
	close(start)
	wg.Wait()
}

func TestGroup_NotACache(t *testing.T) {
	var g Group[int]
	var calls atomic.Int32
	fn := func() (int, error) { return int(calls.Add(1)), nil }

	v1, _, _ := g.Do("k", fn)
	v2, _, _ := g.Do("k", fn)
	if v1 != 1 || v2 != 2 {
		t.Errorf("got %d then %d; a finished call must be forgotten so fn runs again", v1, v2)
	}
}

func TestGroup_KeysAreIndependent(t *testing.T) {
	var g Group[string]
	slowStarted := make(chan struct{})
	go g.Do("slow", func() (string, error) {
		close(slowStarted)
		time.Sleep(300 * time.Millisecond)
		return "", nil
	})
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("Do never ran fn for the first key")
	}

	within(t, 100*time.Millisecond, "Do for a different key", func() {
		g.Do("fast", func() (string, error) { return "x", nil })
	})
}
