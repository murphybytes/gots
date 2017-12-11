package main

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/murphybytes/gots/internal/config"
	"github.com/pkg/errors"
)

func main() {
	config, err := config.New()
	if err != nil {
		fmt.Printf("Program failed: %s\n", errors.Wrap(err, "starting program"))
		os.Exit(1)
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join([]string(config.Kafka.BrokerAddress), ","),
	})
	if err != nil {
		fmt.Printf("Program failed: %s\n", errors.Wrap(err, "starting kafka producer"))
		os.Exit(1)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func(events <-chan kafka.Event) {
		defer wg.Done()
		for event := range events {
			switch ev := event.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Publishing error: %s\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s at offset %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Offset)
				}
			}
		}
	}(p.Events())

	for i := 0; i < 100; i++ {
		for _, topic := range config.Kafka.Topics {
			fmt.Println("pub topic " + topic)
			p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte("hello there")}
		}
	}

	p.Flush(2000)
	p.Close()
	wg.Wait()

}
