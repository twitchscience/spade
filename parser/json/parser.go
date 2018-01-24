package json

import (
	"fmt"
	"time"

	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/parser"
)

// LogParser parses JSON log messages from the edge.
type LogParser struct{}

// Parse turns the raw edge event into a mixpanel event.
func (j *LogParser) Parse(raw parser.Parseable) ([]parser.MixpanelEvent, error) {
	var rawEvent spade.Event
	err := spade.Unmarshal(raw.Data(), &rawEvent)
	if err != nil {
		return []parser.MixpanelEvent{*parser.MakeErrorEvent(raw, "", time.Time{}, rawEvent.EdgeType)}, err
	}

	events, err := parser.DecodeBase64([]byte(rawEvent.Data), &parser.ByteQueryUnescaper{})
	if err != nil {
		return []parser.MixpanelEvent{
			*parser.MakeErrorEvent(raw, rawEvent.Uuid, rawEvent.ReceivedAt, rawEvent.EdgeType),
		}, err
	}

	m := make([]parser.MixpanelEvent, len(events))
	for i, e := range events {
		m[i] = e
		m[i].EventTime = rawEvent.ReceivedAt
		m[i].ClientIP = rawEvent.ClientIp.String()
		m[i].Pstart = raw.StartTime()
		m[i].UserAgent = rawEvent.UserAgent
		m[i].EdgeType = rawEvent.EdgeType
		if len(events) > 1 {
			m[i].UUID = fmt.Sprintf("%s-%d", rawEvent.Uuid, i)
		} else {
			m[i].UUID = rawEvent.Uuid
		}
	}
	return m, nil
}
