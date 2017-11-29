package storage

import (
	"container/list"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	mr "math/rand"
	"testing"
	"time"

	"sync"

	"github.com/murphybytes/gots/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func listToArray(l list.List) []api.Element {
	var result []api.Element
	for elt := l.Front(); elt != nil; elt = elt.Next() {
		result = append(result, elt.Value.(api.Element))
	}
	return result
}

func TestStorageCreationAndClose(t *testing.T) {
	s := New(DefaultMaxAge, DefaultWorkerCount, DefaultChannelBufferSize)
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

	expireOldElements(data, 110)
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

	storage := New(DefaultMaxAge, 10, DefaultChannelBufferSize)
	defer storage.Close()
	var wg sync.WaitGroup
	wg.Add(200)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			key := randomKey()
			// have to use base as current time or all the keys will expire before we can do our tests
			base := time.Now().UnixNano()
			for i := 0; i < 100; i++ {
				ts := mr.Int63() % 100
				storage.Write(key, api.Element{base + ts, nil})
			}
		}()
	}
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			keys := storage.keys()

			for i := 0; i < 100; i++ {
				for _, k := range keys {
					storage.Search(k, NoLowerBound, NoUpperBound)
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
			elts, err := storage.Search(keys[j], NoLowerBound, NoUpperBound)
			require.Nil(t, err)
			assert.Len(t, elts, 100)
			assert.True(t, sorted(elts))
		})
	}
}
