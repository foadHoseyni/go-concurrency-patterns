package conc

import (
	"context"
	"runtime"
	"slices"
	"testing"
	"time"
)

func TestPipeline(t *testing.T) {
	ctx := context.Background()
	got := collect(t, Square(ctx, Generate(ctx, 1, 2, 3, 4)), time.Second)
	if want := []int{1, 4, 9, 16}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPipeline_CancelStopsGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())

	nums := make([]int, 1000)
	for i := range nums {
		nums[i] = i
	}
	out := Square(ctx, Generate(ctx, nums...))
	for i := 0; i < 3; i++ {
		select {
		case <-out:
		case <-time.After(time.Second):
			t.Fatal("no value received")
		}
	}
	time.Sleep(20 * time.Millisecond) // let both stages block on their next send
	cancel()                          // stop reading after 3 values; both stages must exit
	noLeak(t, before)
}
