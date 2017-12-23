package server

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics/discard"
	"github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/murphybytes/gots/api"
	"github.com/murphybytes/gots/internal/service"
	"github.com/murphybytes/gots/internal/service/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"fmt"
)

func createTestServer(lh service.LoginHandler, ah service.AuthHandler, wg sync.WaitGroup) (*grpc.Server, storage.Manager, error) {
	storage := storage.New(storage.Options{
		MaxAge:            time.Hour,
		WorkerCount:       10,
		ChannelBufferSize: 10,
		MessageCounter:    discard.NewCounter(),
	})

	svc := service.New(log.NewNopLogger(), storage, lh)
	gsvr := grpc.NewServer(
		grpc.UnaryInterceptor(
			grpc_auth.UnaryServerInterceptor(
				injectAuthFunctions(ah),
			),
		),
	)
	api.RegisterTimeseriesServiceServer(gsvr, svc)
	listener, err := net.Listen("tcp", ":50001")
	if err != nil {
		return nil, nil, err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		gsvr.Serve(listener)
	}()

	return gsvr, storage, nil
}

func TestTestServer(t *testing.T) {
	var wg sync.WaitGroup
	svr, strg, err := createTestServer(nil, nil, wg)
	require.Nil(t, err)
	strg.Close()
	svr.GracefulStop()
	wg.Wait()
}

func TestLoginNoAuth(t *testing.T) {
	func(f func(t *testing.T)) {
		var wg sync.WaitGroup
		svr, strg, err := createTestServer(nil, nil, wg)
		require.Nil(t, err)
		f(t)
		strg.Close()
		svr.GracefulStop()
		wg.Wait()
	}(func(t *testing.T) {
		conn, err := grpc.Dial(":50001", grpc.WithInsecure())
		require.Nil(t, err)
		defer conn.Close()
		client := api.NewTimeseriesServiceClient(conn)
		req := &api.LoginRequest{"foo", "bar"}
		resp, err := client.Login(context.Background(), req)
		assert.Error(t, err, service.ErrLoginNotAuthorized)
		assert.Nil(t, resp)
	})
}

func TestAuthenticatedRequest(t *testing.T) {
	loginHandler := func(u,p string)(string, error){
		assert.Equal(t, "foo", u)
		assert.Equal(t, "bar", p)
		return "token", nil
	}
	authHandler := func(jwt string) error {
		assert.Equal(t, "token", jwt)
		return nil
	}
	func(f func(t *testing.T)) {
		var wg sync.WaitGroup
		svr, strg, err := createTestServer(loginHandler, authHandler, wg)
		require.Nil(t, err)
		strg.Write("key", time.Now(), []byte("hello there"))
		f(t)
		strg.Close()
		svr.GracefulStop()
		wg.Wait()
	}(func(t *testing.T) {
		conn, err := grpc.Dial(":50001", grpc.WithInsecure())
		require.Nil(t, err)
		defer conn.Close()
		client := api.NewTimeseriesServiceClient(conn)
		req := &api.LoginRequest{UserName: "foo", Password: "bar"}
		resp, err := client.Login(context.Background(), req)
		assert.Nil(t, err )
		assert.NotNil(t, resp)
		assert.Equal(t, "token", resp.Token)

		md := metautils.NiceMD(metadata.Pairs(
			"Authorization", fmt.Sprintf("Bearer %s", resp.Token),
		))

		searchResults, err := client.Search(
			md.ToOutgoing(context.Background()),
			&api.SearchRequest{
				Key:    "key",
				Oldest: uint64(time.Now().Add(-1 * time.Second).UnixNano()),
				Newest: uint64(time.Now().Add(time.Second).UnixNano()),
			},
		)
		require.Nil(t,err)
		require.NotNil(t, searchResults)
		require.Len(t, searchResults.Results.Elements, 1)
		assert.Equal(t, "hello there", string(searchResults.Results.Elements[0].Data))
		assert.Equal(t, api.SearchResponse_OK, searchResults.Status)

	})
}
