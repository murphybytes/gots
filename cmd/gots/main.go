package main

import (
	"fmt"
	"os"

	"github.com/micro/go-micro"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/server"
	"github.com/micro/go-plugins/broker/kafka"
	"github.com/micro/go-plugins/registry/memory"
	"github.com/murphybytes/gots/api"
	"github.com/murphybytes/gots/internal/config"
	"github.com/murphybytes/gots/internal/service/handler"
	"github.com/murphybytes/gots/internal/service/storage"
	"github.com/murphybytes/gots/internal/service/subscriber"
)

func main() {
	config, err := config.New()
	if err != nil {
		fmt.Printf("")
	}

	storage := storage.New(
		config.Storage.MaxAge,
		config.Storage.WorkerCount,
		config.Storage.ChannelBufferSize,
	)

	handler := handler.New(storage)
	subscriber := subscriber.New(storage)

	broker := kafka.NewBroker(
		func(opts *broker.Options) {
			for _, addr := range config.Kafka.BrokerAddress {
				opts.Addrs = append(opts.Addrs, addr)
			}
		},
	)

	service := micro.NewService(
		micro.Name(config.ServiceName),
		micro.Broker(broker),
		micro.Registry(
			memory.NewRegistry(),
		),
	)

	service.Init()

	for _, topic := range config.Kafka.Topics {
		fmt.Println("registering topic " + topic)
		err := micro.RegisterSubscriber(
			topic,
			service.Server(),
			subscriber,
			func(opts *server.SubscriberOptions) {
				opts.Queue = "0"
			},
		)
		if err != nil {
			fmt.Printf("Error: %s", err)
			os.Exit(1)
		}
	}
	fmt.Println("here")
	api.RegisterTimeseriesServiceHandler(service.Server(), handler)

	if err = service.Run(); err != nil {
		fmt.Printf("Error: %s", err)
		os.Exit(1)
	}
}
