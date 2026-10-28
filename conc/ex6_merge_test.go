package conc

import (
	"context"
	"runtime"
	"slices"
	"testing"
	"time"
)

func source(vals ...int) <-chan int {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for _, v := range vals {
			ch <- v
		}
	}()
	return ch
}

func TestMerge(t *testing.T) {
	got := collect(t, Merge(context.Background(), source(1, 2), source(3), source(4, 5, 6)), time.Second)
	slices.Sort(got)
	if want := []int{1, 2, 3, 4, 5, 6}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v (any order)", got, want)
	}
}

func TestMerge_NoInputs(t *testing.T) {
	if got := collect(t, Merge[int](context.Background()), time.Second); len(got) != 0 {
		t.Errorf("got %v, want nothing", got)
	}
}

func TestMerge_Cancel(t *testing.T) {
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	never := make(chan int) // never closed, never sends
	out := Merge(ctx, never, never)
	cancel()
	collect(t, out, time.Second) // output must close after cancel
	noLeak(t, before)
}
