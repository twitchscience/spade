package nginx

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/twitchscience/spade/parser"
)

var MultiEventEscape = []byte{'[', '{'}

func Register() {
	parser.Register("nginx", &NginxLogParser{
		Escaper: &ByteQueryUnescaper{},
	})
}

type NginxLogParser struct {
	Escaper parser.URLEscaper
}

type parseResult struct {
	ip   string
	when string
	data []byte
	uuid string
}

func (p *parseResult) UUID() string {
	return p.uuid
}

func (p *parseResult) Time() string {
	return p.when
}

func (worker *NginxLogParser) decodeData(matches *parseResult) ([]parser.MixpanelEvent, error) {
	data := matches.data
	data, err := worker.Escaper.QueryUnescape(data)
	if err != nil {
		return nil, err
	}
	// We dont have to allocate a new byte array here because the len(dst) < len(src)
	n, b64Err := base64.StdEncoding.Decode(data, data)
	if b64Err != nil {
		return nil, b64Err
	}
	var events []parser.MixpanelEvent
	if n > 1 && bytes.Equal(data[:2], MultiEventEscape) {
		err = json.Unmarshal(data[:n], &events)
		if err != nil {
			return nil, err
		}
	} else {
		event := new(parser.MixpanelEvent)
		err = json.Unmarshal(data[:n], event)
		if err != nil {
			return nil, err
		}
		events = []parser.MixpanelEvent{
			*event,
		}
	}
	return events, nil
}

// ParseRequest -> parser.MixpanelEvent
func (worker *NginxLogParser) Parse(line parser.Parseable) ([]parser.MixpanelEvent, error) {
	matches := LexLine(line.Data())

	events, err := worker.decodeData(matches)
	if err != nil {
		return []parser.MixpanelEvent{*parser.MakeErrorEvent(line, matches)}, err
	}

	m := make([]parser.MixpanelEvent, len(events))
	for i, e := range events {
		m[i] = e
		m[i].EventTime = json.Number(matches.when)
		m[i].ClientIp = matches.ip
		m[i].Pstart = line.StartTime()
		if len(events) > 1 {
			m[i].UUID = fmt.Sprintf("%s-%d", matches.uuid, i)
		} else {
			m[i].UUID = matches.uuid
		}
	}
	return m, nil
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
