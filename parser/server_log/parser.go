package server_log

import (
	"encoding/json"
	"fmt"

	"github.com/twitchscience/spade/parser"
)

func Register() {
	parser.Register("server_log", &serverLogParser{
		escaper: &parser.ByteQueryUnescaper{},
	})
}

type serverLogParser struct {
	escaper parser.URLEscaper
}

// ParseRequest -> parser.MixpanelEvent
func (worker *serverLogParser) Parse(raw parser.Parseable) ([]parser.MixpanelEvent, error) {
	matches := lexLine(raw.Data())

	events, err := parser.DecodeBase64(matches, worker.escaper)
	if err != nil {
		return []parser.MixpanelEvent{
			*parser.MakeErrorEvent(raw, matches.uuid, matches.when),
		}, err
	}

	m := make([]parser.MixpanelEvent, len(events))
	for i, e := range events {
		m[i] = e
		m[i].EventTime = json.Number(matches.when)
		m[i].ClientIp = matches.ip
		m[i].Pstart = raw.StartTime()
		if len(events) > 1 {
			m[i].UUID = fmt.Sprintf("%s-%d", matches.uuid, i)
		} else {
			m[i].UUID = matches.uuid
		}
	}
	return m, nil
}
