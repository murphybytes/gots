package service

import (
	"context"
	"github.com/go-kit/kit/log"
	"time"
	//"github.com/go-kit/kit/metrics"
	"github.com/murphybytes/gots/api"
)

type middleware func(service TimeseriesService) TimeseriesService

type loggingMiddleware struct {
	next   TimeseriesService
	logger log.Logger
}

func newLoggingMiddleware(logger log.Logger) middleware {
	return func(next TimeseriesService) TimeseriesService {
		return &loggingMiddleware{
			next:   next,
			logger: log.With(logger, "compenent", "service"),
		}

	}
}

func (mw *loggingMiddleware) Search(ctx context.Context, req *api.SearchRequest) (resp *api.SearchResponse, err error) {
	defer func(begin time.Time) {
		mw.logger.Log(
			"method", "Search",
			"duration", time.Since(begin),
			"err", err,
		)
	}(time.Now())
	resp, err = mw.Search(ctx, req)
	return resp, err
}
