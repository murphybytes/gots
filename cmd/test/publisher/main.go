package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/micro/go-micro"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-plugins/broker/kafka"
	"github.com/murphybytes/gots/api"
)

func main() {

	b := kafka.NewBroker(
		func(o *broker.Options) {
			o.Addrs = []string{
				"192.168.1.146:9092",
			}
		},
	)
	service := micro.NewService(
		micro.Name("publisher"),
		micro.Broker(b),
	)
	service.Init()
	pub := micro.NewPublisher("test", service.Client())

	msg := &api.Message{
		Key:       "foo",
		Timestamp: time.Now().UnixNano(),
		Data:      []byte("this is the payload"),
	}

	err := pub.Publish(context.Background(), msg)
	if err != nil {
		fmt.Printf("Error: %s", err)
		os.Exit(1)
	}

	time.Sleep(time.Second)

}
