// Package storage handles in memory storage and searching for time series elements. Multiple goroutines handle search and
// storage of time series elements each of which is mapped to a unique key. Keys and associated elements are distributed to
// goroutines to reduce lock contention.
package storage

import (
	"container/list"
	"io"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/murphybytes/gots/api"
	"github.com/murphybytes/gots/internal/service"
)

const (
	expirationFrequency = 5 * time.Second
	// NoUpperBound indicates that there is no upper bound constraint on a series of elements
	NoUpperBound uint64 = 0x7FFFFFFFFFFFFFFF
	// NoLowerBound indicates that there is not a lower bound constraint on a series of elements
	NoLowerBound uint64 = 0
	// DefaultMaxAge default amount of time we keep elements
	DefaultMaxAge = 61 * time.Minute
	// DefaultWorkerCount is the default number of workers to use to handle data
	DefaultWorkerCount = 256
	// DefaultChannelBufferSize is the default buffer size for work channels
	DefaultChannelBufferSize = 100
)

type Writer interface {
	Write(key string, elt api.Element)
}

type Searcher interface {
	Search(key string, first, last uint64) ([]api.Element, error)
}

type Manager interface {
	io.Closer
	Searcher
	Writer
}

// Element storage is optimized for inserts
type elementMap map[string]*list.List
type operation func(data elementMap)

type storage struct {
	wait        sync.WaitGroup
	close       chan struct{}
	work        []chan operation
	maxAge      time.Duration
	workerCount int
}

// New creates in memory storage for time series data. Elements older than maxAge will be discarded.  workerCount is the
// number of goroutines to handle messages and search requests.
// jobBufferSize is the number of jobs that can be queued up so jobs can be processed asynchronously.
func New(maxAge time.Duration, workerCount, channelBufferSize int) *storage {
	s := &storage{
		close:       make(chan struct{}),
		maxAge:      maxAge,
		workerCount: workerCount,
	}
	s.work = make([]chan operation, workerCount)
	s.wait.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		s.work[i] = make(chan operation, channelBufferSize)
		go func(work <-chan operation, close <-chan struct{}) {
			defer s.wait.Done()
			ticker := time.Tick(expirationFrequency)
			data := make(elementMap)
			for {
				select {
				case <-close:
					return
				case job := <-work:
					job(data)
				case <-ticker:
					cutOff := time.Now().Add(-1 * s.maxAge).UnixNano()
					expireOldElements(data, cutOff)
				}
			}
		}(s.work[i], s.close)
	}
	return s

}

// Write adds an element to the time series for a key.
func (s *storage) Write(key string, elt api.Element) {
	partition := s.calculateWorkerPartition(key)
	s.work[partition] <- func(data elementMap) {
		var (
			pl    *list.List
			found bool
		)
		if pl, found = data[key]; !found {
			pl = new(list.List)
			pl.PushBack(elt)
			data[key] = pl
			return
		}

		insert(pl, elt)
	}
}

type searchResult struct {
	err  error
	elts []api.Element
}

// Search returns a set of elements for a particular key between first and last times. First and last are unix time in
// nanoseconds.
func (s *storage) Search(key string, first, last uint64) ([]api.Element, error) {
	if first > last {
		return nil, &service.ErrorInvalidSearch{}
	}
	responseChan := make(chan searchResult)
	partition := s.calculateWorkerPartition(key)
	s.work[partition] <- func(data elementMap) {
		if elts, ok := data[key]; ok {
			responseChan <- searchResult{elts: search(elts, int64(first), int64(last))}
			return
		}
		responseChan <- searchResult{err: &service.ErrorNotFound{Key: key}}
	}
	result := <-responseChan
	return result.elts, result.err
}

func (s *storage) Close() error {
	close(s.close)
	s.wait.Wait()
	return nil
}

func (s *storage) keys() []string {
	var result []string
	for _, w := range s.work {
		ch := make(chan []string)
		w <- func(data elementMap) {
			var result []string
			for k := range data {
				result = append(result, k)
			}
			ch <- result
		}
		r := <-ch
		close(ch)
		result = append(result, r...)
	}
	return result
}

func (s *storage) calculateWorkerPartition(key string) int {
	cs := xxhash.ChecksumString32(key)
	return int(cs) % s.workerCount
}

func insert(l *list.List, elt api.Element) {
	for curr := l.Back(); curr != nil; curr = curr.Prev() {
		existingElt := curr.Value.(api.Element)
		if elt.Timestamp >= existingElt.Timestamp {
			l.InsertAfter(elt, curr)
			return
		}
	}
	l.PushFront(elt)
}

func search(elts *list.List, first, last int64) []api.Element {
	if elts.Len() == 0 {
		return nil
	}
	lastTick := elts.Back().Value.(api.Element)
	if lastTick.Timestamp < first {
		return nil
	}
	firstTick := elts.Front().Value.(api.Element)
	if firstTick.Timestamp >= last {
		return nil
	}
	result := make([]api.Element, 0, elts.Len())

	for elt := elts.Front(); elt != nil; elt = elt.Next() {
		tick := elt.Value.(api.Element)
		if tick.Timestamp >= first && tick.Timestamp < last {
			result = append(result, tick)
		}
	}
	return result
}

func expireOldElements(data elementMap, firstTimestamp int64) {
	var empties []string
	for key, l := range data {

		for {
			curr := l.Front()
			if curr == nil {
				break
			}
			elt := curr.Value.(api.Element)
			if elt.Timestamp < firstTimestamp {
				l.Remove(curr)
				continue
			}
			break
		}

		if l.Len() == 0 {
			empties = append(empties, key)
		}
	}
	for _, k := range empties {
		delete(data, k)
	}
}
