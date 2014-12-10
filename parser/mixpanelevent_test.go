package parser

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/twitchscience/spade/reporter"
)

type mixpanelEventType int

const (
	BAD_ENCODING mixpanelEventType = iota
	PANIC
	ERROR
	ERROR_LONG_UUID
	ERROR_EMPTY_UUID
	ERROR_EMPTY_TIME
	ERROR_INVALID_TIME
)

var (
	epoch    = time.Unix(0, 0)
	uuid     = "123abc"
	longUuid = randomString(68)
	when     = fmt.Sprintf("%d", receivedAt.Unix()) // from parser_test.go
)

func randomString(n int) string {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal("while generating random string:", err)
	}
	return string(b)
}

func makeEvent(t mixpanelEventType) *MixpanelEvent {
	switch t {
	case BAD_ENCODING:
		return MakeBadEncodedEvent()
	case PANIC:
		return MakePanicedEvent(&logLine{})
	case ERROR:
		return MakeErrorEvent(&logLine{}, uuid, when)
	case ERROR_LONG_UUID:
		return MakeErrorEvent(&logLine{}, longUuid, when)
	case ERROR_EMPTY_UUID:
		return MakeErrorEvent(&logLine{}, "", when)
	case ERROR_EMPTY_TIME:
		return MakeErrorEvent(&logLine{}, uuid, "")
	case ERROR_INVALID_TIME:
		return MakeErrorEvent(&logLine{}, uuid, "abc")
	}
	return nil
}

func TestMixpanelEvent(t *testing.T) {
	tests := []struct {
		t         mixpanelEventType
		pstart    time.Time
		eventTime json.Number
		uuid      string
		clientIp  string
		eventName string
		rawProps  json.RawMessage
		failMode  reporter.FailMode
	}{
		{
			t:         BAD_ENCODING,
			pstart:    epoch, // method uses time.Now()
			eventTime: json.Number("0"),
			uuid:      "error",
			clientIp:  "",
			eventName: "Unknown",
			rawProps:  json.RawMessage{},
			failMode:  reporter.FAILED_TRANSPORT,
		},
		{
			t:         PANIC,
			pstart:    receivedAt, // from parser_test.go
			eventTime: json.Number("0"),
			uuid:      "error",
			clientIp:  "",
			eventName: "Unknown",
			rawProps:  json.RawMessage([]byte{}),
			failMode:  reporter.PANICED_IN_PROCESSING,
		},
		{
			t:         ERROR,
			pstart:    receivedAt, // from parser_test.go
			eventTime: json.Number(when),
			uuid:      uuid,
			clientIp:  "",
			eventName: "Unknown",
			rawProps:  json.RawMessage{},
			failMode:  reporter.UNABLE_TO_PARSE_DATA,
		},
		{
			t:         ERROR_LONG_UUID,
			pstart:    receivedAt, // from parser_test.go
			eventTime: json.Number(when),
			uuid:      "error",
			clientIp:  "",
			eventName: "Unknown",
			rawProps:  json.RawMessage{},
			failMode:  reporter.UNABLE_TO_PARSE_DATA,
		},
		{
			t:         ERROR_EMPTY_UUID,
			pstart:    receivedAt, // from parser_test.go
			eventTime: json.Number(when),
			uuid:      "error",
			clientIp:  "",
			eventName: "Unknown",
			rawProps:  json.RawMessage{},
			failMode:  reporter.UNABLE_TO_PARSE_DATA,
		},
		{
			t:         ERROR_EMPTY_TIME,
			pstart:    receivedAt, // from parser_test.go
			eventTime: json.Number("0"),
			uuid:      uuid,
			clientIp:  "",
			eventName: "Unknown",
			rawProps:  json.RawMessage{},
			failMode:  reporter.UNABLE_TO_PARSE_DATA,
		},
		{
			t:         ERROR_INVALID_TIME,
			pstart:    receivedAt, // from parser_test.go
			eventTime: json.Number("0"),
			uuid:      uuid,
			clientIp:  "",
			eventName: "Unknown",
			rawProps:  json.RawMessage{},
			failMode:  reporter.UNABLE_TO_PARSE_DATA,
		},
	}

	for _, tt := range tests {
		me := makeEvent(tt.t)
		if tt.pstart != epoch && tt.pstart != me.Pstart {
			t.Fatalf("mixpanelevent: Expected pstart of %s, got %s", tt.pstart, me.Pstart)
		}
		if tt.eventTime != me.EventTime {
			t.Fatalf("mixpanelevent: Expected EventTime of %s, got %s", tt.eventTime, me.EventTime)
		}
		if tt.uuid != me.UUID {
			t.Fatalf("mixpanelevent: Expected UUID of %s, got %s", tt.uuid, me.UUID)
		}
		if tt.clientIp != me.ClientIp {
			t.Fatalf("mixpanelevent: Expected ClientIp of %s, got %s", tt.clientIp, me.ClientIp)
		}
		if tt.eventName != me.Event {
			t.Fatalf("mixpanelevent: Expected Event of %s, got %s", tt.eventName, me.Event)
		}
		if !reflect.DeepEqual(tt.rawProps, me.Properties) {
			t.Fatalf("mixpanelevent: Expected Properties of %v, got %v", tt.rawProps, me.Properties)
		}
		if tt.failMode != me.Failure {
			t.Fatalf("mixpanelevent: Expected Failure of %t, got %t", tt.failMode, me.Failure)
		}
	}
}
