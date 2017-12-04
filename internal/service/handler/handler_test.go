package handler

import (
	"context"
	"fmt"
	"testing"

	"github.com/murphybytes/gots/api"
	"github.com/murphybytes/gots/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSearcher struct {
	results []api.Element
	err     error
}

func (ms *mockSearcher) Search(key string, first, last uint64) ([]api.Element, error) {
	return ms.results, ms.err
}

func TestNotFound(t *testing.T) {
	mock := mockSearcher{
		err: &service.ErrorNotFound{},
	}
	h := New(&mock)
	var response api.SearchResponse
	err := h.Search(
		context.Background(),
		&api.SearchRequest{
			Key:    "foo",
			Oldest: api.NoLowerBound,
			Newest: api.NoUpperBound,
		},
		&response,
	)
	require.Nil(t, err)
	assert.Equal(t, response.Results.Key, "foo")
	assert.Equal(t, response.Status, api.SearchResponse_NOT_FOUND)
}

func TestInvalidSearch(t *testing.T) {
	mock := mockSearcher{
		err: &service.ErrorInvalidSearch{},
	}
	h := New(&mock)
	var response api.SearchResponse
	err := h.Search(
		context.Background(),
		&api.SearchRequest{
			Key:    "foo",
			Oldest: api.NoUpperBound,
			Newest: api.NoLowerBound,
		},
		&response,
	)
	require.Nil(t, err)
	assert.Equal(t, response.Results.Key, "foo")
	assert.Equal(t, response.Status, api.SearchResponse_INVALID_ARGUMENTS)
}

func TestErrorSearch(t *testing.T) {
	errTest := fmt.Errorf("error")
	mock := mockSearcher{
		err: errTest,
	}
	h := New(&mock)
	var response api.SearchResponse
	err := h.Search(
		context.Background(),
		&api.SearchRequest{
			Key:    "foo",
			Oldest: api.NoLowerBound,
			Newest: api.NoUpperBound,
		},
		&response,
	)
	require.NotNil(t, err)
}

func TestValidSearch(t *testing.T) {
	mock := mockSearcher{
		results: []api.Element{
			{1, nil},
			{2, nil},
			{3, nil},
		},
	}
	h := New(&mock)
	var resp api.SearchResponse
	err := h.Search(
		context.Background(),
		&api.SearchRequest{
			Key:    "foo",
			Oldest: api.NoLowerBound,
			Newest: api.NoUpperBound,
		},
		&resp,
	)
	require.Nil(t, err)
	require.NotNil(t, resp.Results)
	assert.Equal(t, resp.Results.Key, "foo")
	assert.Equal(t, resp.Status, api.SearchResponse_OK)
	assert.Len(t, resp.Results.Elements, 3)
}
