package syncx

import (
	"maps"
	"sync"
)

// Exercise 1 — protecting shared state with sync.Mutex
//
// Stats counts events per key and is used from many goroutines at once
// (think: request counters per endpoint).
//
//	Inc(key)     adds 1 to key's counter.
//	Get(key)     returns key's current count (0 if never incremented).
//	Snapshot()   returns ALL counters as a map. The caller may modify the
//	             returned map without affecting Stats, and Stats keeps
//	             changing without affecting the returned map.
//
// Afterwards, be ready to answer: would sync.RWMutex help here? Could you use
// sync/atomic instead, and what would that cost you?
type Stats struct {
	Counter map[string]int
	mu      sync.Mutex
}

func NewStats() *Stats {
	return &Stats{Counter: make(map[string]int)}
}

func (s *Stats) Inc(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Counter[key]++
}

func (s *Stats) Get(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Counter[key]
}

func (s *Stats) Snapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapShot := maps.Clone(s.Counter)
	return snapShot
}
