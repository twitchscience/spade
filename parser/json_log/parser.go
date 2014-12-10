package json_log

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

type jsonLogParser struct{}

func Register() {
	parser.Register("json_log", &jsonLogParser{})
}

func (j *jsonLogParser) Parse(raw parser.Parseable) ([]parser.MixpanelEvent, error) {
	var rawEvent spade.Event
	err := spade.Unmarshal(raw.Data(), &rawEvent)
	if err != nil {
		return []parser.MixpanelEvent{*parser.MakeErrorEvent(raw, "", "")}, err
	}

	parsedEvent := &jsonLogEvent{event: rawEvent}
	events, err := parser.DecodeBase64(parsedEvent, &parser.ByteQueryUnescaper{})
	if err != nil {
		return []parser.MixpanelEvent{
			*parser.MakeErrorEvent(raw, rawEvent.Uuid, parsedEvent.Time()),
		}, err
	}

	m := make([]parser.MixpanelEvent, len(events))
	for i, e := range events {
		m[i] = e
		m[i].EventTime = json.Number(parsedEvent.Time())
		m[i].ClientIp = rawEvent.ClientIp.String()
		m[i].Pstart = raw.StartTime()
		if len(events) > 1 {
			m[i].UUID = fmt.Sprintf("%s-%d", parsedEvent.UUID(), i)
		} else {
			m[i].UUID = parsedEvent.UUID()
		}
	}
	return m, nil
}
