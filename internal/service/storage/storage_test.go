package storage

import (
	"container/list"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	mr "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/metrics/discard"
	"github.com/murphybytes/gots/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

func listToArray(l list.List) []api.Element {
	var result []api.Element
	for elt := l.Front(); elt != nil; elt = elt.Next() {
		result = append(result, elt.Value.(api.Element))
	}
	return result
}

func TestStorageCreationAndClose(t *testing.T) {
	opts := Options{
		MaxAge:            DefaultMaxAge,
		WorkerCount:       DefaultWorkerCount,
		ChannelBufferSize: DefaultChannelBufferSize,
		MessageCounter:    discard.NewCounter(),
	}
	s := New(opts)
	require.NotNil(t, s)
	s.Close()
}

func TestStorageInsertion(t *testing.T) {
	tt := []struct {
		desc     string
		inserts  []api.Element
		expected []api.Element
	}{
		{
			"single",
			[]api.Element{
				{100, nil},
			},
			[]api.Element{
				{100, nil},
			},
		},
		{
			"inverted",
			[]api.Element{
				{110, nil},
				{100, nil},
			},
			[]api.Element{
				{100, nil},
				{110, nil},
			},
		},
		{
			"late",
			[]api.Element{
				{110, nil},
				{120, nil},
				{100, nil},
			},
			[]api.Element{
				{100, nil},
				{110, nil},
				{120, nil},
			},
		},
		{
			"ordered",
			[]api.Element{
				{100, nil},
				{110, nil},
				{120, nil},
			},
			[]api.Element{
				{100, nil},
				{110, nil},
				{120, nil},
			},
		},
		{
			"reversed",
			[]api.Element{
				{130, nil},
				{120, nil},
				{110, nil},
				{100, nil},
			},
			[]api.Element{
				{100, nil},
				{110, nil},
				{120, nil},
				{130, nil},
			},
		},
		{
			"duplicate",
			[]api.Element{
				{130, nil},
				{130, []byte{'a'}},
				{120, nil},
				{110, nil},
				{100, nil},
			},
			[]api.Element{
				{100, nil},
				{110, nil},
				{120, nil},
				{130, nil},
				{130, []byte{'a'}},
			},
		},
	}

	for i := range tt {
		t.Run(tt[i].desc, func(t *testing.T) {
			var l list.List
			for _, elt := range tt[i].inserts {
				insert(&l, elt)
			}
			actual := listToArray(l)
			assert.Equal(t, tt[i].expected, actual)
		})
	}
}

func TestStorageExpiration(t *testing.T) {
	elts := []api.Element{
		{100, nil},
		{110, nil},
		{120, nil},
		{130, nil},
	}
	var listA, listB, listC, listD list.List
	for i := range elts {
		listA.PushBack(elts[i])
		if i > 0 {
			listB.PushBack(elts[i])
		}
	}
	listD.PushBack(elts[0])

	data := elementMap{
		"A": &listA,
		"B": &listB,
		"C": &listC,
		"D": &listD,
	}

	expireOldElements(data, 110, nil)
	// first elt removed
	require.Equal(t, 3, data["A"].Len())
	require.Equal(t, int64(110), data["A"].Front().Value.(api.Element).Timestamp)
	// not elts removed
	require.Equal(t, 3, data["B"].Len())
	require.Equal(t, int64(110), data["B"].Front().Value.(api.Element).Timestamp)
	_, present := data["C"]
	// list removed in both cases
	require.False(t, present)
	_, present = data["D"]
	require.False(t, present)
}

func TestStorage(t *testing.T) {
	randomKey := func() string {
		key := make([]byte, 8)
		rand.Read(key)
		return base64.StdEncoding.EncodeToString(key)
	}
	sorted := func(elts []api.Element) bool {
		const unassigned int64 = -1
		last := unassigned
		for _, elt := range elts {
			if last == unassigned {
				last = elt.Timestamp
				continue
			}
			if last > elt.Timestamp {
				return false
			}
			last = elt.Timestamp
		}
		return true
	}
	opts := Options{
		MaxAge:            DefaultMaxAge,
		WorkerCount:       10,
		ChannelBufferSize: DefaultChannelBufferSize,
		MessageCounter:    discard.NewCounter(),
	}

	storage := New(opts)
	defer storage.Close()
	var wg sync.WaitGroup
	wg.Add(200)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			key := randomKey()
			// have to use base as current time or all the keys will expire before we can do our tests
			base := time.Now()
			for i := 0; i < 100; i++ {
				ts := time.Duration(mr.Int63() % 100)
				storage.Write(key, base.Add(ts), nil)
			}
		}()
	}
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			keys := storage.keys()

			for i := 0; i < 100; i++ {
				for _, k := range keys {
					storage.Search(k, api.NoLowerBound, api.NoUpperBound)
				}
			}
		}()
	}
	wg.Wait()
	keys := storage.keys()
	for i := 0; i < 10; i++ {
		// sample keys
		j := mr.Int() % 100
		t.Run(fmt.Sprintf("sampled_%d", j), func(t *testing.T) {
			elts, err := storage.Search(keys[j], api.NoLowerBound, api.NoUpperBound)
			require.Nil(t, err)
			assert.Len(t, elts, 100)
			assert.True(t, sorted(elts))
		})
	}
}

func TestStorageSearch(t *testing.T) {
	base := time.Now().UnixNano()
	// TODO: more search tests, no lower bound, upper bound and lower bound with no upper bound, bounds outside of elts

	tt := []struct {
		key      string
		desc     string
		inserts  []api.Element
		expected []api.Element
		first    uint64
		last     uint64
		err      error
	}{

		{
			"A",
			"no_bounds",
			[]api.Element{
				{Timestamp: base + 130},
				{Timestamp: base + 130},
				{Timestamp: base + 120},
				{Timestamp: base + 110},
				{Timestamp: base + 100},
			},
			[]api.Element{
				{Timestamp: base + 100},
				{Timestamp: base + 110},
				{Timestamp: base + 120},
				{Timestamp: base + 130},
				{Timestamp: base + 130},
			},
			api.NoLowerBound,
			api.NoUpperBound,
			nil,
		},
		{
			"B",
			"normal_search",
			[]api.Element{
				{Timestamp: base + 130},
				{Timestamp: base + 130},
				{Timestamp: base + 120},
				{Timestamp: base + 110},
				{Timestamp: base + 100},
			},
			[]api.Element{
				{Timestamp: base + 110},
				{Timestamp: base + 120},
			},
			uint64(base + 110),
			uint64(base + 130),
			nil,
		},
		{
			"C",
			"invalid_search",
			[]api.Element{
				{Timestamp: base + 130},
				{Timestamp: base + 130},
				{Timestamp: base + 120},
				{Timestamp: base + 110},
				{Timestamp: base + 100},
			},
			nil,
			uint64(base + 130),
			uint64(base + 110),
			&ErrorInvalidSearch{},
		},
	}

	for _, tc := range tt {
		t.Run(tc.desc, func(t *testing.T) {
			stg := New(Options{
				MaxAge:            DefaultMaxAge,
				WorkerCount:       10,
				ChannelBufferSize: DefaultChannelBufferSize,
				MessageCounter:    discard.NewCounter(),
			})
			defer stg.Close()
			for _, elt := range tc.inserts {
				stg.Write(tc.key, epoch.Add(time.Duration(elt.Timestamp)), nil)
			}
			actual, err := stg.Search(tc.key, tc.first, tc.last)
			require.Equal(t, tc.err, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
