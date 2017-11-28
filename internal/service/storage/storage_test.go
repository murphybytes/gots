package storage

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func TestStorageCreationAndClose(t *testing.T) {
	s := New(DefaultMaxAge, DefaultWorkerCount, DefaultChannelBufferSize)
	require.NotNil(t,s)
	s.Close()
}
