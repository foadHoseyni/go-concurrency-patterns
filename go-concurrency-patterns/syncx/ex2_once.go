package syncx

import (
	"sync"
)

// Exercise 2 — load something exactly once (sync.Once / sync.OnceValues)
//
// A Lazy loads an expensive value (e.g. config from disk, a DB connection
// pool) the first time anyone asks for it. If 100 goroutines call Get at the
// same moment, load must run exactly ONCE and all of them get the same
// result. The error is remembered too: if loading failed, every Get returns
// that same error (no retry).
//
// Two acceptable approaches: sync.Once + fields, or sync.OnceValues (Go 1.21).
// Know both.
type Lazy[T any] struct {
	Load func() (T, error)
	val  T
	err  error
	once sync.Once
}

func NewLazy[T any](load func() (T, error)) *Lazy[T] {

	return &Lazy[T]{Load: load}
}

func (l *Lazy[T]) Get() (T, error) {

	l.once.Do(func() {
		l.val, l.err = l.Load()
	})
	return l.val, l.err
}
