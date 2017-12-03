// Package subscriber handles incoming messages from Kafka.
package subscriber

import (
	"context"

	"github.com/murphybytes/gots/api"
	"github.com/murphybytes/gots/internal/service/storage"
)

type sub struct {
	storage storage.Writer
}

// New creates a subscriber which writes messages received from publisher to storage.
func New(storage storage.Writer) *sub {
	return &sub{
		storage: storage,
	}
}
// Process writes messages to storage.
func (s *sub) Process(ctx context.Context, msg *api.Message) {
	s.storage.Write(msg.Key, api.Element{Timestamp: msg.Timestamp, Data: msg.Data})
}
