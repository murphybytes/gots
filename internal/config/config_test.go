package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"time"
)

func TestConfig(t *testing.T) {
	os.Setenv("GOTS_BROKER_ADDRESS", "192.168.1.1:9093")
	os.Setenv("GOTS_TOPICS", "topic1,topic2")
	os.Setenv("GOTS_MAX_ELEMENT_AGE", "20s")
	os.Setenv("GOTS_WORKER_COUNT", "300")
	os.Setenv("GOTS_CHANNEL_BUFFER_SIZE", "123")

	v, e := New()
	require.Nil(t, e)

	assert.Equal(t, list{"192.168.1.1:9093"}, v.Kafka.BrokerAddress)
	assert.Equal(t, list{"topic1", "topic2"}, v.Kafka.Topics)
	assert.Equal(t, 20*time.Second, v.Storage.MaxAge)
	assert.Equal(t, 300, v.Storage.WorkerCount)
	assert.Equal(t, 123, v.Storage.ChannelBufferSize)
}
