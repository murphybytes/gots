// Package subscriber handles incoming messages from Kafka.
package subscriber

import (
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-kit/kit/log"
	"github.com/murphybytes/gots/internal/config"
	"github.com/murphybytes/gots/internal/service/storage"
)

type svr struct {
	closer chan struct{}
	wait   sync.WaitGroup
	logger log.Logger
}

// New creates a subscriber which writes messages received from publisher to storage.
func New(wtr storage.Writer, cfg *kafka.ConfigMap, logger log.Logger) (*svr, error) {
	svr := &svr{
		closer: make(chan struct{}),
		logger: log.With(logger, "component", "subscriber"),
	}

	config, err := config.New()
	if err != nil {
		return nil, err
	}
	c, err := kafka.NewConsumer(
		cfg,
	)
	if err != nil {
		return nil, err
	}
	if err = c.SubscribeTopics(config.Kafka.Topics, nil); err != nil {
		return nil, err
	}
	svr.wait.Add(1)

	go func(closer <-chan struct{}, wtr storage.Writer, consumer *kafka.Consumer) {
		defer svr.wait.Done()
		svr.logger.Log("msg", "starting")
		defer svr.logger.Log("msg", "shutting down")

		for {
			select {
			case <-closer:
				break
			case evt := <-c.Events():
				switch msg := evt.(type) {
				case kafka.AssignedPartitions:
					svr.logger.Log(
						"msg", "assigned partitions",
						"details", fmt.Sprintf("%v", msg),
					)
					c.Assign(msg.Partitions)
				case kafka.RevokedPartitions:
					svr.logger.Log(
						"msg", "unassign partitions",
						"details", fmt.Sprintf("%v", msg),
					)
					c.Unassign()
				case *kafka.Message:
					wtr.Write(string(msg.Key), msg.Timestamp, msg.Value)
				case kafka.PartitionEOF:
					svr.logger.Log(
						"msg", "partition eof",
						"details", fmt.Sprintf("%v", msg),
					)
				case kafka.Error:
					svr.logger.Log(
						"msg", "error",
						"err", fmt.Sprintf("%v", msg),
					)
					break
				}
			}
		}

		c.Close()

	}(svr.closer, wtr, c)

	return svr, nil

}

func (s *svr) Close() error {
	close(s.closer)
	s.wait.Wait()
	return nil
}
