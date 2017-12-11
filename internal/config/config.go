// Package config reads application environment variables.
package config

import (
	"strings"
	"time"

	"github.com/joeshaw/envdecode"
	"github.com/pkg/errors"
)

type list []string

// Kafka settings for kafka cluster we'll subscribe to.
type kafka struct {
	// BrokerAddress is a comma seperated list of IP_ADDRESS:PORT of Kafka servers.
	BrokerAddress list `env:"GOTS_BROKER_ADDRESS"`
	// Topics comma delimited list of topics to subscribe to.
	Topics list `env:"GOTS_TOPICS"`
	// GroupID client group ID string
	GroupID string `env:"GOTS_GROUP_ID"`
	// SessionTimeout length of time to wait for session to timeout.
	SessionTimeout time.Duration `env:"GETS_SESSION_TIMEOUT,default=6000ms"`
}

func (k *kafka) TimeoutMS() int {
	return int(k.SessionTimeout.Nanoseconds() / 1000000)
}

// Storage configuration for local time series storage
type storage struct {
	// MaxAge defines how long to keep time series elements.  Uses time.ParseDuration formatting.
	MaxAge time.Duration `env:"GOTS_MAX_ELEMENT_AGE,default=1h,strict"`
	// WorkerCount is the number of goroutines that will process incoming messages.
	WorkerCount int `env:"GOTS_WORKER_COUNT,default=125"`
	// ChannelBufferSize is the size of the channel used by each worker.  Bigger numbers may increase throughput.
	// at a cost of higher latency
	ChannelBufferSize int `env:"GOTS_CHANNEL_BUFFER_SIZE,default=1000"`
}

type server struct {
	// Address is the IP address and port that the server will listen on
	Address string `env:"GOTS_SERVER_ADDRESS"`
}

type values struct {
	ServiceName string `env:"GOTS_SERVICE_NAME,default=gots"`
	Kafka       kafka
	Storage     storage
	Server      server
}

// New reads environment variables for the application and returns a structure containing these values.
func New() (*values, error) {
	var vals values
	if err := envdecode.Decode(&vals); err != nil {
		return nil, errors.Wrap(err, "reading configuration from environment")
	}
	return &vals, nil
}

func (t *list) Decode(v string) error {
	items := strings.Split(v, ",")
	*t = append(*t, items...)
	return nil
}

func (t *list) String() string {
	return strings.Join(*t, ",")
}
