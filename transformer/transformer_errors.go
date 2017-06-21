package transformer

import (
	"errors"
)

var (
	// ErrEmptyRequest indicates the Event field is not set.
	ErrEmptyRequest = errors.New("Event field is not set")
	// ErrInvalidEdgeType indicates the event has an unknown edge type.
	ErrInvalidEdgeType = errors.New("Edge type is invalid")
)

// ErrNotTracked indicates an event type is not tracked.
type ErrNotTracked struct {
	What string
}

// ErrSkippedColumn indicates an event is missing one or more columns.
type ErrSkippedColumn struct {
	What string
}

// Error returns information on which event type is not being tracked.
func (t ErrNotTracked) Error() string {
	return t.What
}

// Error returns information on one column which is missing.
func (t ErrSkippedColumn) Error() string {
	return t.What
}
