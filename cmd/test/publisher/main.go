package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/murphybytes/gots/internal/config"
	"github.com/pkg/errors"
)

func main() {
	var keyCount int
	flag.IntVar(&keyCount, "keys", 100, "Count of keys to generate")
	flag.Parse()
	sig := make(chan os.Signal, 1)
	closer := make(chan struct{})
	signal.Notify(sig, os.Interrupt)

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
	wg.Add(2)

	go func(events <-chan kafka.Event, closer <-chan struct{}) {
		defer wg.Done()
		counter := 0
		for {
			select {
			case event := <-events:
				switch ev := event.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						fmt.Printf("Publishing error: %s\n", ev.TopicPartition.Error)
					} else {
						//fmt.Printf("Delivered message to topic %s at offset %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Offset)
						counter++
						if (counter % 1000) == 0 {
							fmt.Printf("Sending Messsage %d\n", counter)
						}

					}
				}
			case <-closer:
				return
			}

		}
	}(p.Events(), closer)

	go func(out chan<- *kafka.Message, closer <-chan struct{}) {
		defer wg.Done()
		var keys []string

		for i := 0; i < keyCount; i++ {
			keys = append(keys, strconv.Itoa(i))
		}

		for {
			for _, topic := range config.Kafka.Topics {
				out <- &kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &topic,
						Partition: kafka.PartitionAny,
					},
					Value:     []byte("hello there"),
					Key:       key(keys),
					Timestamp: time.Now(),
				}
			}

			select {
			case <-closer:
				return
			default:
			}
		}
	}(p.ProduceChannel(), closer)

	<-sig
	close(closer)

	p.Flush(2000)
	wg.Wait()
}

func key(keys []string) []byte {
	n := rand.Int()
	return []byte(keys[n%len(keys)])
}
