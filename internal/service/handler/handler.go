// Package handler handles gRPC calls that expose gots api.
package handler

import (
	"context"
	"github.com/murphybytes/gots/api"
)

type handler struct{}

// NewHandler creates a new time series handler.
func New() (*handler, error) {
	return new(handler), nil
}

// Search for time series elements by key and timestamp range
func (h *handler) Search(ctx context.Context, req *api.SearchRequest, resp *api.SearchResponse) error {
	return nil
}
