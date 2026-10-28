package conc

import (
	"fmt"
	"testing"
	"time"
)

func TestFetchAll(t *testing.T) {
	ids := make([]string, 10)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
	}
	fetch := func(id string) int {
		time.Sleep(50 * time.Millisecond)
		var n int
		fmt.Sscan(id, &n)
		return n * n
	}

	start := time.Now()
	got := FetchAll(ids, fetch)
	elapsed := time.Since(start)

	if len(got) != len(ids) {
		t.Fatalf("got %d results, want %d", len(got), len(ids))
	}
	for i, v := range got {
		if v != i*i {
			t.Errorf("result[%d] = %d, want %d (order must match ids)", i, v, i*i)
		}
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("took %v; calls should run concurrently (~50ms)", elapsed)
	}
}

func TestFetchAll_Empty(t *testing.T) {
	if got := FetchAll(nil, func(string) int { return 1 }); len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}
