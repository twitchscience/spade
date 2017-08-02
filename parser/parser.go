package parser

import "time"

// Parseable is a byte stream to be parsed associated with a time.
type Parseable interface {
	Data() []byte
	StartTime() time.Time
}

// Parser is an interface for turning Parseables into one or more MixpanelEvents.
type Parser interface {
	Parse(Parseable) ([]MixpanelEvent, error)
}

// URLEscaper is an interface for unescape URL query encoding in a byte stream.
type URLEscaper interface {
	QueryUnescape([]byte) ([]byte, error)
}

// ParseResult is a base 64-encoded byte array with a UUID and time attached.
type ParseResult interface {
	Data() []byte
	UUID() string
	Time() string
}
