package parser

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/twitchscience/spade/reporter"
)

var MultiEventEscape = []byte{'[', '{'}

type NginxLogParser struct {
	Reporter reporter.Reporter
	Escaper  URLEscaper
}

type parseResult struct {
	Ip   string
	Time string
	Data []byte
	UUID string
}

type URLEscaper interface {
	QueryUnescape([]byte) ([]byte, error)
}

func BuildSpadeParser(r reporter.Reporter) Parser {
	return &NginxLogParser{
		Reporter: r,
		Escaper:  &ByteQueryUnescaper{},
	}
}

func (p *parseResult) Equal(other *parseResult) bool {
	return p.Ip == other.Ip &&
		p.Time == other.Time &&
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

func MakeErrorEvent(line *ParseRequest) *MixpanelEvent {
	return &MixpanelEvent{
		Pstart:     line.Pstart,
		EventTime:  json.Number(0),
		UUID:       "error",
		ClientIp:   "",
		Event:      "Unknown",
		Properties: json.RawMessage{},
		Failure:    reporter.UNABLE_TO_PARSE_DATA,
	}
}

func (worker *NginxLogParser) decodeData(matches *parseResult) ([]MixpanelEvent, error) {
	data := matches.Data
	data, err := worker.Escaper.QueryUnescape(data)
	if err != nil {
		return nil, err
	}
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
		return []MixpanelEvent{*MakeErrorEvent(line)}, err
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

type StringQueryUnescaper struct{}

func (s *StringQueryUnescaper) QueryUnescape(q []byte) ([]byte, error) {
	out, err := url.QueryUnescape(string(q))
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}

type ByteQueryUnescaper struct{}

func ishex(c byte) bool {
	switch {
	case '0' <= c && c <= '9':
		return true
	case 'a' <= c && c <= 'f':
		return true
	case 'A' <= c && c <= 'F':
		return true
	}
	return false
}

func unhex(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

func (s *ByteQueryUnescaper) QueryUnescape(q []byte) ([]byte, error) {
	return unescape(q)
}

func unescape(s []byte) ([]byte, error) {
	// Count %, check that they're well-formed.
	n := 0
	hasPlus := false
	for i := 0; i < len(s); {
		switch s[i] {
		case '%':
			n++
			if i+2 >= len(s) || !ishex(s[i+1]) || !ishex(s[i+2]) {
				s = s[i:]
				if len(s) > 3 {
					s = s[0:3]
				}
				return nil, errors.New("invalid URL escape")
			}
			i += 3
		case '+':
			hasPlus = true
			i++
		default:
			i++
		}
	}

	if n == 0 && !hasPlus {
		return s, nil
	}

	t := make([]byte, len(s)-2*n)
	j := 0
	for i := 0; i < len(s); {
		switch s[i] {
		case '%':
			t[j] = unhex(s[i+1])<<4 | unhex(s[i+2])
			j++
			i += 3
		case '+':
			t[j] = ' '
			j++
			i++
		default:
			t[j] = s[i]
			j++
			i++
		}
	}
	return t, nil
}
