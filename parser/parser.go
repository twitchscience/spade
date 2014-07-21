package parser

import (
	"encoding/json"
	"time"

	"github.com/TwitchScience/spade/reporter"
)

type MixpanelEvent struct {
	Pstart     time.Time         // the time that we started processing
	EventTime  json.Number       // the time that the server recieved the event
	UUID       string            // UUID of the event as assigned by the edge
	ClientIp   string            // the ipv4 of the client
	Event      string            // the type of the event
	Properties json.RawMessage   // the raw bytes of the json properties sub object
	Failure    reporter.FailMode // a flag for failure modes
}

type ParseRequest struct {
	Target []byte
	Pstart time.Time
}

type Parser interface {
	Parse(*ParseRequest) ([]MixpanelEvent, error)
}
