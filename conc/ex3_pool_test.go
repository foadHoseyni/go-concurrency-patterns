package conc

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	jobs := make([]int, 20)
	for i := range jobs {
		jobs[i] = i
	}
	var running, maxRunning atomic.Int32
	fn := func(j int) int {
		n := running.Add(1)
		for {
			m := maxRunning.Load()
			if n <= m || maxRunning.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		return j * 10
	}

	start := time.Now()
	got := Pool(jobs, 4, fn)
	elapsed := time.Since(start)

	if len(got) != len(jobs) {
		t.Fatalf("got %d results, want %d", len(got), len(jobs))
	}
	for i, v := range got {
		if v != i*10 {
			t.Errorf("result[%d] = %d, want %d", i, v, i*10)
		}
	}
	if m := maxRunning.Load(); m != 4 {
		t.Errorf("max concurrent = %d, want exactly 4 workers", m)
	}
	if elapsed > 250*time.Millisecond {
		t.Errorf("took %v; 20 jobs x 20ms on 4 workers should take ~100ms", elapsed)
	}
}
