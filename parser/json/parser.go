package json

import (
	"encoding/json"
	"fmt"

	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/parser"
)

type jsonLogEvent struct {
	event spade.Event
}

func (j *jsonLogEvent) Data() []byte {
	return []byte(j.event.Data)
}

func (j *jsonLogEvent) UUID() string {
	return j.event.Uuid
}

func (j *jsonLogEvent) Time() string {
	return fmt.Sprintf("%d", j.event.ReceivedAt.Unix())
}

// LogParser parses JSON log messages from the edge.
type LogParser struct{}

// Parse turns the raw edge event into a mixpanel event.
func (j *LogParser) Parse(raw parser.Parseable) ([]parser.MixpanelEvent, error) {
	var rawEvent spade.Event
	err := spade.Unmarshal(raw.Data(), &rawEvent)
	if err != nil {
		return []parser.MixpanelEvent{*parser.MakeErrorEvent(raw, "", "", rawEvent.EdgeType)}, err
	}

	parsedEvent := &jsonLogEvent{event: rawEvent}
	events, err := parser.DecodeBase64(parsedEvent, &parser.ByteQueryUnescaper{})
	if err != nil {
		return []parser.MixpanelEvent{
			*parser.MakeErrorEvent(raw, rawEvent.Uuid, parsedEvent.Time(), rawEvent.EdgeType),
		}, err
	}

	m := make([]parser.MixpanelEvent, len(events))
	for i, e := range events {
		m[i] = e
		m[i].EventTime = json.Number(parsedEvent.Time())
		m[i].ClientIP = rawEvent.ClientIp.String()
		m[i].Pstart = raw.StartTime()
		m[i].UserAgent = rawEvent.UserAgent
		m[i].EdgeType = rawEvent.EdgeType
		if len(events) > 1 {
			m[i].UUID = fmt.Sprintf("%s-%d", parsedEvent.UUID(), i)
		} else {
			m[i].UUID = parsedEvent.UUID()
		}
	}
	return m, nil
}
