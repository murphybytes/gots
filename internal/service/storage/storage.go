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
			data := make(elementMap)
			for {
				select {
				case <-close:
					return
				case job := <-work:
					job(data)
				}
			}

		}(s.work[i], s.close)
	}
	return s

}

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

		s.insert(pl, elt)
	}
}

func (s *storage) Search(key string, first, last uint64) ([]api.Element, error) {
	return nil, nil
}

func (s *storage) Close() error {
	return nil
}

func (s *storage) calculateWorkerPartition(key string) int {
	cs := xxhash.ChecksumString32(key)
	return int(cs) % s.workerCount
}

func (s *storage) insert(l *list.List, elt api.Element) {
	cutOff := time.Now().Add(-1 * s.maxAge).UnixNano()
	// search backwards for insertion
	//var mark *list.Element
	for curr := l.Back(); curr != nil; curr = curr.Prev() {
		currElt := curr.Value.(api.Element)
		if currElt.Timestamp < cutOff {
			l.Remove(curr)
			curr = l.Back()
			break
		}

	}
}
