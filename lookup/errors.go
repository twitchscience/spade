package lookup

import "errors"

var (
	// ErrTooManyRequests - Trying to send too many fetch requests
	ErrTooManyRequests = errors.New("Trying to send too many fetch requests")

	// ErrExtractingValue - Failed to extract a value with given arguments
	ErrExtractingValue = errors.New("Failed to extract value with given arguments")
)
