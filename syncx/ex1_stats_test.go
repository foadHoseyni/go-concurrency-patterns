package syncx

import (
	"sync"
	"testing"
)

func TestStats_Concurrent(t *testing.T) {
	s := NewStats()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				s.Inc("orders")
				if j%2 == 0 {
					s.Inc("carts")
				}
				_ = s.Get("orders")
				_ = s.Snapshot()
			}
		}()
	}
	wg.Wait()

	if got := s.Get("orders"); got != 10000 {
		t.Errorf("orders = %d, want 10000 (lost updates?)", got)
	}
	if got := s.Get("carts"); got != 5000 {
		t.Errorf("carts = %d, want 5000", got)
	}
	if got := s.Get("never"); got != 0 {
		t.Errorf("unknown key = %d, want 0", got)
	}
}

func TestStats_SnapshotIsACopy(t *testing.T) {
	s := NewStats()
	s.Inc("a")
	snap := s.Snapshot()
	if snap["a"] != 1 {
		t.Fatalf("snapshot = %v, want a:1", snap)
	}

	snap["a"] = 99 // caller changes its copy...
	if got := s.Get("a"); got != 1 {
		t.Errorf("modifying the snapshot changed Stats: a = %d, want 1", got)
	}

	s.Inc("a") // ...and Stats keeps changing
	if snap["a"] != 99 {
		t.Errorf("Stats changes leaked into an old snapshot")
	}
}
