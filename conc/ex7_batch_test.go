package conc

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestBatch_BySize(t *testing.T) {
	got := collect(t, Batch(context.Background(), source(1, 2, 3, 4, 5), 2, time.Second), 2*time.Second)
	want := [][]int{{1, 2}, {3, 4}, {5}}
	if !slices.EqualFunc(got, want, slices.Equal[[]int]) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// send fails the test instead of hanging if nobody reads from in.
func send(t *testing.T, in chan<- int, v int) {
	t.Helper()
	select {
	case in <- v:
	case <-time.After(time.Second):
		t.Fatalf("nobody is reading the input channel (sending %d)", v)
	}
}

func TestBatch_ByTime(t *testing.T) {
	in := make(chan int)
	out := Batch(context.Background(), in, 10, 50*time.Millisecond)

	send(t, in, 1)
	select {
	case b := <-out:
		if !slices.Equal(b, []int{1}) {
			t.Errorf("got %v, want [1]", b)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("partial batch not sent after maxWait")
	}

	send(t, in, 2)
	send(t, in, 3)
	close(in)
	rest := collect(t, out, time.Second)
	if len(rest) != 1 || !slices.Equal(rest[0], []int{2, 3}) {
		t.Errorf("got %v, want [[2 3]]", rest)
	}
}

func TestBatch_NoEmptyBatches(t *testing.T) {
	in := make(chan int)
	out := Batch(context.Background(), in, 3, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond) // several maxWait periods with no input
	close(in)
	if got := collect(t, out, time.Second); len(got) != 0 {
		t.Errorf("got %v, want no batches at all", got)
	}
}
