package syncx

import "sync"

// Exercise 4 — de-duplicate concurrent work (singleflight)
//
// Ten requests arrive at once for the same product page. Without
// de-duplication, all ten hit the database. With a Group, the first caller
// for a key runs fn; callers that arrive for the SAME key while it is still
// running don't run fn — they wait for that result and get it too.
//
//	Do(key, fn) returns fn's value and error, plus shared=true if the result
//	was given to more than one caller.
//
// Once a call finishes, the key is forgotten: the next Do runs fn again
// (this is NOT a cache). Different keys never wait for each other.
//
// You'll need: a mutex, a map of in-flight calls, and a way for waiters to
// block until the call is done (sync.WaitGroup or a channel you close).
type Group[V any] struct {
	calls map[string]*Call[V]
	mu    sync.Mutex
}

type Call[V any] struct {
	done chan struct{}
	dups int
	v    V
	err  error
}

func (g *Group[V]) Do(key string, fn func() (V, error)) (V, error, bool) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*Call[V])
	}

	if call, ok := g.calls[key]; ok {
		call.dups++
		g.mu.Unlock()
		<-call.done
		return call.v, call.err, true
	}

	c := Call[V]{
		done: make(chan struct{}),
	}
	g.calls[key] = &c

	g.mu.Unlock()

	c.v, c.err = fn()

	g.mu.Lock()
	delete(g.calls, key)
	shared := c.dups > 0
	close(c.done)
	g.mu.Unlock()

	return c.v, c.err, shared
}
