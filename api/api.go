// Package api contains protobuffer definitions that expose gRPC endpoints.
package api

const (
	// NoUpperBound indicates that there is no upper bound constraint on a series of elements
	NoUpperBound uint64 = 0x7FFFFFFFFFFFFFFF
	// NoLowerBound indicates that there is not a lower bound constraint on a series of elements
	NoLowerBound uint64 = 0
)
