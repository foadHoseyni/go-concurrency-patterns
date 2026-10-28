package conc

import (
	"sync"
)

// Exercise 1 — goroutines + sync.WaitGroup
//
// FetchAll calls fetch once for every id, all at the same time, and returns
// the results in the SAME ORDER as ids. With 10 ids and a fetch that takes
// 50ms, the whole call should take about 50ms, not 500ms.

func FetchAll(ids []string, fetch func(id string) int) []int {
	var wg sync.WaitGroup
	result := make([]int, len(ids))
	for idx, id := range ids {

		wg.Go(func() {
			result[idx] = fetch(id)
		})
	}
	wg.Wait()
	return result
}
