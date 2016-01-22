package parser

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
)

var multiEventEscape = []byte{'[', '{'}

func DecodeBase64(matches ParseResult, escaper URLEscaper) ([]MixpanelEvent, error) {
	data := matches.Data()
	data, err := escaper.QueryUnescape(data)
	if err != nil {
		return nil, err
	}

	var n int
	// We dont have to allocate a new byte array here because the len(dst) < len(src)
	if bytes.IndexAny(data, "-_") == -1 {
		n, err = base64.StdEncoding.Decode(data, data)
	} else {
		n, err = base64.URLEncoding.Decode(data, data)
	}

	if err != nil {
		return nil, err
	}

	var events []MixpanelEvent
	if n > 1 && bytes.Equal(data[:2], multiEventEscape) {
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

type ByteQueryUnescaper struct{}

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
