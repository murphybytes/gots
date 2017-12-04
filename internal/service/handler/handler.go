// Package handler handles gRPC calls that expose gots api.
package handler

import (
	"context"

	"github.com/murphybytes/gots/api"

	"github.com/murphybytes/gots/internal/service"
	"github.com/murphybytes/gots/internal/service/storage"
)

type handler struct {
	storage storage.Searcher
}

// NewHandler creates a new time series handler.
func New(storage storage.Searcher) *handler {
	return &handler{
		storage: storage,
	}
}

// Search for time series elements by key and timestamp range
func (h *handler) Search(ctx context.Context, req *api.SearchRequest, resp *api.SearchResponse) error {
	resp.Results = &api.Series{
		Key: req.Key,
	}

	elts, err := h.storage.Search(req.Key, req.Oldest, req.Newest)
	switch err.(type) {
	case service.KeyNotFound:
		resp.Status = api.SearchResponse_NOT_FOUND
		return nil
	case service.InvalidSearch:
		resp.Status = api.SearchResponse_INVALID_ARGUMENTS
		return nil
	case nil:
	default:
		return err
	}
	for _, elt := range elts {
		resp.Results.Elements = append(resp.Results.Elements, &elt)
	}

	return nil
}
