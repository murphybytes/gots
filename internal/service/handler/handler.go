// Package handler handles gRPC calls that expose gots api.
package handler

import (
	"context"

	"github.com/murphybytes/gots/api"
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
	return nil
}
