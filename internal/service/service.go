package service

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/murphybytes/gots/api"
	"github.com/murphybytes/gots/internal/service/storage"
)

// TimeseriesService defines endpoint for grpc calls.
type TimeseriesService interface {
	Search(context.Context, *api.SearchRequest) (*api.SearchResponse, error)
}

type svc struct {
	searcher storage.Searcher
}

func New(logger log.Logger, searcher storage.Searcher) TimeseriesService {
	var s TimeseriesService
	{
		s = &svc{
			searcher: searcher,
		}
		s = newLoggingMiddleware(logger)(s)
	}
	return s
}

// Search for time series elements by key and timestamp range
func (s *svc) Search(ctx context.Context, req *api.SearchRequest) (*api.SearchResponse, error) {
	var resp api.SearchResponse
	resp.Results = &api.Series{
		Key: req.Key,
	}

	elts, err := s.searcher.Search(req.Key, req.Oldest, req.Newest)
	switch err.(type) {
	case storage.KeyNotFound:
		resp.Status = api.SearchResponse_NOT_FOUND
		return &resp, nil
	case storage.InvalidSearch:
		resp.Status = api.SearchResponse_INVALID_ARGUMENTS
		return &resp, nil
	case nil:
	default:
		return nil, err
	}
	for _, elt := range elts {
		resp.Results.Elements = append(resp.Results.Elements, &elt)
	}

	return &resp, nil
}
