package service

import (
	"context"
	"github.com/go-kit/kit/log"
	"time"
	//"github.com/go-kit/kit/metrics"
	"github.com/murphybytes/gots/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"fmt"
)


// AuthHandler is a function to handle authorization. The service will extract a jwt token from the bearer
// header and pass it to AuthHandler.  AuthHandler returns nil if authorized, otherwise an error.
type AuthHandler func(jwt string) error
// LoginHandler takes a user name and password and returns a jwt token that the client will use in subsequent
// requests. LogHandler returns ErrNotAuthorized if authorization fails. Other errors maybe returned for server side problems.
type LoginHandler func(user, password string)(jwtToken string, err error)
// ErrLoginNotAuthorized is returned from LoginHandler if invalid credentials are presented.
var ErrLoginNotAuthorized = status.Error(codes.Unauthenticated, "Login credentials not authorized" )


type contextKey string

const (
	// Authenticated is a context key used to indicate if caller is authenticated
	authenticated contextKey = "authenticated"
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
	resp, err = mw.next.Search(ctx, req)
	return resp, err
}

func(mw *loggingMiddleware) Login(ctx context.Context, req *api.LoginRequest)(resp *api.LoginResponse, err error) {
	defer func(begin time.Time) {
		mw.logger.Log(
			"method", "Login",
			"duration", time.Since(begin),
			"err", err,
		)
	}(time.Now())
	resp, err = mw.next.Login(ctx, req)
	return resp, err
}


type authMiddleware struct {
	next TimeseriesService
}

func SetAuthenticated(ctx context.Context, auth bool ) context.Context {
	return context.WithValue(ctx, authenticated, auth )
}

func Authenticated(ctx context.Context ) bool {
	v := ctx.Value(authenticated)
	if v == nil   {
		return false
	}
	b, ok := v.(bool)
	if !ok {
		return false
	}
	return b
}

func (mw *authMiddleware) Search(ctx context.Context, req *api.SearchRequest) (*api.SearchResponse, error) {
	if !Authenticated(ctx) {
		return nil, status.Error(codes.Unauthenticated, "unauthorized")
	}
	return mw.next.Search(ctx, req)
}

func(mw *authMiddleware) Login(ctx context.Context, req *api.LoginRequest)(*api.LoginResponse, error) {
	return mw.next.Login(ctx, req)
}
