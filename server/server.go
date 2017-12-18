// Package server contains the time series database functionality so that it can easily be included in other processes.
// For example, gots gets it's configuration settings from environment variables. If you wanted to get your configuration
// settings from some other source such as Consul or the command line, you could include this package and create your
// own process to host it.
package server

import (
	"io"
	"os"
	"time"

	"context"
	"net"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/discard"
	"github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/murphybytes/gots/api"
	"github.com/murphybytes/gots/internal/service"
	"github.com/murphybytes/gots/internal/service/storage"
	"github.com/murphybytes/gots/internal/service/subscriber"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultMaxAge            = time.Hour
	defaultWorkerCount       = 128
	defaultChannelBufferSize = 512
	defaultGRPCListenAddress = ":8088"
)

type Option func(*svr)

// ElementMaxAge set the age at which time series elements will be discarded
func ElementMaxAge(age time.Duration) Option {
	return func(s *svr) {
		s.storageMaxAge = age
	}
}

// StorageWorkerCount set the number of goroutines that will process incoming time series elements
func StorageWorkerCount(workers int) Option {
	return func(s *svr) {
		s.storageWorkersCount = workers
	}
}

// ExpirationCallback provide a function to handle time series elements when they have reached their expiration
// date
func ExpirationCallback(handler storage.ExpiryHandler) Option {
	return func(s *svr) {
		s.expiryHandler = handler
	}
}

// StorageChannelBuffer is the number of elements that can be backed up in a channel that feeds a storage worker. This
// can help throughput by asynchronously processing incoming Kafka messages.
func StorageChannelBuffer(size int) Option {
	return func(s *svr) {
		s.storageChannelBufferSize = size
	}
}

// WithLogger provide your own kit logger, by default logs will be written to stdout.
func WithLogger(log log.Logger) Option {
	return func(s *svr) {
		s.logger = log
	}
}

// ListenAddress is the IP address that the grpc server will listen on
func ListenAddress(ip string) Option {
	return func(s *svr) {
		s.listenAddress = ip
	}
}

// ExpiredElementHandler function to handle expired time series elements.
func ExpiredElementHandler(hnd storage.ExpiryHandler) Option {
	return func(s *svr) {
		s.expiryHandler = hnd
	}
}

// MessageCounter count incoming messages.
func MessageCounter(counter metrics.Counter) Option {
	return func(s *svr) {
		s.messageCounter = counter
	}
}

// WantAuth enables jwt based authentication for the server.  A login handler takes a user name and password and
// if authorized returns a token that will be passed to the server in subsequent requests from the client.  The
// auth handler receives this token and uses it to authorize requests.
func WantAuth(auth service.AuthHandler, login service.LoginHandler) Option {
	return func(s *svr) {
		s.authHandler = auth
		s.loginHandler = login
	}
}

type svr struct {
	storageMaxAge            time.Duration
	storageWorkersCount      int
	storageChannelBufferSize int
	expiryHandler            storage.ExpiryHandler
	storage                  io.Closer
	subscriber               io.Closer
	logger                   log.Logger
	listenAddress            string
	messageCounter           metrics.Counter
	authHandler              service.AuthHandler
	loginHandler             service.LoginHandler
}

// Run starts processing time series messages and exposes them via grpc endpoint. Run is a blocking call.
func Run(kcfg *kafka.ConfigMap, opts ...Option) error {
	var err error
	s := &svr{
		storageMaxAge:            defaultMaxAge,
		storageWorkersCount:      defaultWorkerCount,
		storageChannelBufferSize: defaultChannelBufferSize,
		listenAddress:            defaultGRPCListenAddress,
		messageCounter:           discard.NewCounter(),
	}
	for _, opt := range opts {
		opt(s)
	}
	storage := storage.New(
		storage.Options{
			MaxAge:            s.storageMaxAge,
			WorkerCount:       s.storageWorkersCount,
			ChannelBufferSize: s.storageChannelBufferSize,
			OnExpire:          s.expiryHandler,
			MessageCounter:    s.messageCounter,
		},
	)
	defer storage.Close()

	if s.logger == nil {
		s.logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
	}

	subs, err := subscriber.New(storage, kcfg, s.logger)
	if err != nil {
		return err
	}
	defer subs.Close()

	svc := service.New(s.logger, storage)
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpc_auth.UnaryServerInterceptor(injectAuthFunctions(s.authHandler))),
	)
	api.RegisterTimeseriesServiceServer(grpcServer, svc)

	listener, err := net.Listen("tcp", s.listenAddress)
	if err != nil {
		return err
	}

	if err = grpcServer.Serve(listener); err != nil {
		return err
	}

	return nil
}

func injectAuthFunctions(h service.AuthHandler) grpc_auth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		// If no auth handler exists we are always authenticated
		if h == nil {
			return service.SetAuthenticated(ctx, true), nil
		}
		token, err := grpc_auth.AuthFromMD(ctx, "bearer")
		if err != nil {
			return service.SetAuthenticated(ctx, false ), nil
		}
		if err = h(token); err != nil {
			return service.SetAuthenticated(ctx,false), nil
		}

		return service.SetAuthenticated(ctx,true), nil
	}
}
