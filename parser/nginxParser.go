package parser

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/twitchscience/spade/reporter"
)

var MultiEventEscape = []byte{'[', '{'}

type NginxLogParser struct {
	Reporter reporter.Reporter
}

type parseResult struct {
	Ip   string
	Time string
	Data []byte
	UUID string
}

func BuildSpadeParser(r reporter.Reporter) Parser {
	return &NginxLogParser{
		Reporter: r,
	}
}

func (p *parseResult) Equal(other *parseResult) bool {
	return p.Ip == other.Ip &&
		p.Time == other.Time &&
		p.UUID == other.UUID &&
		bytes.Equal(p.Data, other.Data)
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

func MakePanicedEvent(line *ParseRequest) *MixpanelEvent {
	return &MixpanelEvent{
		Pstart:     line.Pstart,
		EventTime:  json.Number(0),
		UUID:       "error",
		ClientIp:   "",
		Event:      "Unknown",
		Properties: json.RawMessage(line.Target),
		Failure:    reporter.PANICED_IN_PROCESSING,
	}
}

func MakeErrorEvent(line *ParseRequest, matches *parseResult) *MixpanelEvent {
	if matches.UUID == "" {
		matches.UUID = "error"
	}
	if matches.Time == "" {
		matches.Time = "0"
	}
	t, ok := strconv.Atoi(matches.Time)
	if ok != nil {
		t = 0
	}
	return &MixpanelEvent{
		Pstart:     line.Pstart,
		EventTime:  json.Number(t),
		UUID:       matches.UUID,
		ClientIp:   "",
		Event:      "Unknown",
		Properties: json.RawMessage{},
		Failure:    reporter.UNABLE_TO_PARSE_DATA,
	}
}

func (worker *NginxLogParser) decodeData(matches *parseResult) ([]MixpanelEvent, error) {
	var err error
	data := matches.Data

	// We dont have to allocate a new byte array here because the len(dst) < len(src)
	n, b64Err := base64.StdEncoding.Decode(data, data)
	if b64Err != nil {
		return nil, b64Err
	}
	var events []MixpanelEvent
	if n > 1 && bytes.Equal(data[:2], MultiEventEscape) {
		err = json.Unmarshal(data[:n], &events)
		if err != nil {
			return nil, err
		}
	} else {
		event := new(MixpanelEvent)
		err = json.Unmarshal(data[:n], event)
		if err != nil {
			return nil, err
		}
		events = []MixpanelEvent{
			*event,
		}
	}
	return events, nil
}

// ParseRequest -> MixpanelEvent
func (worker *NginxLogParser) Parse(line *ParseRequest) ([]MixpanelEvent, error) {
	matches := LexLine(line.Target)

	events, err := worker.decodeData(matches)
	if err != nil {
		return []MixpanelEvent{*MakeErrorEvent(line, matches)}, err
	}

	m := make([]MixpanelEvent, len(events))
	for i, e := range events {
		m[i] = e
		m[i].EventTime = json.Number(matches.Time)
		m[i].ClientIp = matches.Ip
		m[i].Pstart = line.Pstart
		if len(events) > 1 {
			m[i].UUID = fmt.Sprintf("%s-%d", matches.UUID, i)
		} else {
			m[i].UUID = matches.UUID
		}
	}
	if len(m) > 1 {
		worker.Reporter.IncrementExpected(len(m) - 1)
	}
	return m, nil
}
