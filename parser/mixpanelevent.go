package parser

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/twitchscience/spade/reporter"
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

func MakeBadEncodedEvent() *MixpanelEvent {
	return &MixpanelEvent{
		Pstart:     time.Now(),
		EventTime:  json.Number(0),
		UUID:       "error",
		ClientIp:   "",
		Event:      "Unknown",
		Properties: json.RawMessage{},
		Failure:    reporter.FAILED_TRANSPORT,
	}
}

func MakePanicedEvent(line Parseable) *MixpanelEvent {
	return &MixpanelEvent{
		Pstart:     line.StartTime(),
		EventTime:  json.Number(0),
		UUID:       "error",
		ClientIp:   "",
		Event:      "Unknown",
		Properties: json.RawMessage(line.Data()),
		Failure:    reporter.PANICED_IN_PROCESSING,
	}
}

func MakeErrorEvent(line Parseable, res ParseResult) *MixpanelEvent {
	uuid := res.UUID()
	if uuid == "" || len(uuid) > 64 {
		uuid = "error"
	}
	when := res.Time()
	if when == "" {
		when = "0"
	}
	t, ok := strconv.Atoi(when)
	if ok != nil {
		t = 0
	}
	return &MixpanelEvent{
		Pstart:     line.StartTime(),
		EventTime:  json.Number(t),
		UUID:       uuid,
		ClientIp:   "",
		Event:      "Unknown",
		Properties: json.RawMessage{},
		Failure:    reporter.UNABLE_TO_PARSE_DATA,
	}
}
