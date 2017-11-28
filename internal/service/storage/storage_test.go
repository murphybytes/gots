package storage

import (
	"container/list"
	"github.com/murphybytes/gots/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
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
