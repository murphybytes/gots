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
	"github.com/go-kit/kit/metrics"
	"github.com/murphybytes/gots/api"
)

const (
	expirationFrequency = 5 * time.Second

	// DefaultMaxAge default amount of time we keep elements
	DefaultMaxAge = 61 * time.Minute
	// DefaultWorkerCount is the default number of workers to use to handle data
	DefaultWorkerCount = 256
	// DefaultChannelBufferSize is the default buffer size for work channels
	DefaultChannelBufferSize = 100
)

// Writer this that write time series data associated with key at time ts.
type Writer interface {
	Write(key string, ts time.Time, data []byte)
}

// Searcher returns time series elements associated with key between first and last times. Times are represented
// as the number of nanoseconds since January 1, 1970 UTC.
type Searcher interface {
	Search(key string, first, last uint64) ([]api.Element, error)
}

// Manager contains Search, Write and Close.
type Manager interface {
	io.Closer
	Searcher
	Writer
}

// ExpiryHandler is a callback that will receive time series elements when they expire.  This can be used
// to aggregate and persist old elements.
type ExpiryHandler func(key string, elt api.Element)

// Element storage is optimized for inserts
type elementMap map[string]*list.List
type operation func(data elementMap)

type storage struct {
	wait  sync.WaitGroup
	close chan struct{}
	work  []chan operation
	opts  Options
}

// Options for storage of time series.
type Options struct {
	// MaxAge time series elements older than this will be discarded.
	MaxAge time.Duration
	// WorkerCount is the number of goroutines that process incoming messages.
	WorkerCount int
	// ChannelBufferSize is the number of jobs that can be buffered in the jobs channel to improve throughput and async processing.
	ChannelBufferSize int
	// OnExpire is an optional method that is called when a time series element expires. This could be used to
	// aggregate expiring elements into courser granularity or to write to persistent storage.
	OnExpire ExpiryHandler
	// MessageCounter keeps tally of the number of messages that have arrived.
	MessageCounter metrics.Counter
}

// New creates in memory storage for time series data.
func New(opts Options) *storage {
	s := &storage{
		close: make(chan struct{}),
		opts:  opts,
	}
	s.work = make([]chan operation, opts.WorkerCount)
	s.wait.Add(opts.WorkerCount)

	for i := 0; i < opts.WorkerCount; i++ {
		s.work[i] = make(chan operation, opts.ChannelBufferSize)
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
					cutOff := time.Now().Add(-1 * opts.MaxAge).UnixNano()
					expireOldElements(data, cutOff, opts.OnExpire)
				}
			}
		}(s.work[i], s.close)
	}
	return s

}

// Write adds an element to the time series for a key.
func (s *storage) Write(key string, ts time.Time, data []byte) {
	s.opts.MessageCounter.Add(1)
	newElt := api.Element{Timestamp: ts.UnixNano(), Data: data}
	partition := s.calculateWorkerPartition(key)
	s.work[partition] <- func(elts elementMap) {
		var (
			pl    *list.List
			found bool
		)
		if pl, found = elts[key]; !found {
			pl = new(list.List)
			pl.PushBack(newElt)
			elts[key] = pl
			return
		}

		insert(pl, newElt)
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
		return nil, &ErrorInvalidSearch{}
	}
	responseChan := make(chan searchResult)
	partition := s.calculateWorkerPartition(key)
	s.work[partition] <- func(data elementMap) {
		if elts, ok := data[key]; ok {
			responseChan <- searchResult{elts: search(elts, int64(first), int64(last))}
			return
		}
		responseChan <- searchResult{err: &ErrorNotFound{Key: key}}
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
	return int(cs) % s.opts.WorkerCount
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

func expireOldElements(data elementMap, firstTimestamp int64, onExpire ExpiryHandler) {
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
				if onExpire != nil {
					onExpire(key, elt)
				}
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
