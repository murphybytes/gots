package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/murphybytes/gots/internal/config"
	"github.com/murphybytes/gots/server"
)

func main() {
	config, err := config.New()
	if err != nil {
		fmt.Printf("Error fetching environment: %s", err)
		os.Exit(1)
	}

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":               config.Kafka.BrokerAddress.String(),
		"group.id":                        config.Kafka.GroupID,
		"session.timeout.ms":              config.Kafka.TimeoutMS(),
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config": kafka.ConfigMap{
			"auto.offset.reset": "earliest",
		},
	}

	err = server.Run(
		kafkaConfig,
		server.ListenAddress(config.Server.Address),
	)
	if err != nil {
		fmt.Printf("Serve exited with error: %s", err)
		os.Exit(1)
	}
}
