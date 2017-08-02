package parser

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/reporter"
)

type mixpanelEventType int

const (
	BadEncoding mixpanelEventType = iota
	Panic
	Error
	ErrorLongUUID
	ErrorEmptyUUID
	ErrorEmptyTime
	ErrorInvalidTime
	ErrorInvalidEdgeType
)

var (
	receivedAt      = time.Now()
	epoch           = time.Unix(0, 0)
	uuid            = "123abc"
	longUUID        = randomString(68)
	when            = fmt.Sprintf("%d", receivedAt.Unix())
	unknownEdgeType = "Unknown"
)

type logLine struct{}

func (l *logLine) Data() []byte {
	return []byte{}
}

func (l *logLine) StartTime() time.Time {
	return receivedAt
}

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
	case BadEncoding:
		return makeBadEncodedEvent()
	case Panic:
		return MakePanickedEvent(&logLine{})
	case Error:
		return MakeErrorEvent(&logLine{}, uuid, when, spade.INTERNAL_EDGE)
	case ErrorLongUUID:
		return MakeErrorEvent(&logLine{}, longUUID, when, spade.INTERNAL_EDGE)
	case ErrorEmptyUUID:
		return MakeErrorEvent(&logLine{}, "", when, spade.INTERNAL_EDGE)
	case ErrorEmptyTime:
		return MakeErrorEvent(&logLine{}, uuid, "", spade.INTERNAL_EDGE)
	case ErrorInvalidTime:
		return MakeErrorEvent(&logLine{}, uuid, "abc", spade.INTERNAL_EDGE)
	case ErrorInvalidEdgeType:
		return MakeErrorEvent(&logLine{}, uuid, when, unknownEdgeType)
	}
	return nil
}

func TestMixpanelEvent(t *testing.T) {
	tests := []struct {
		t         mixpanelEventType
		pstart    time.Time
		eventTime json.Number
		uuid      string
		clientIP  string
		eventName string
		edgeType  string
		rawProps  json.RawMessage
		failMode  reporter.FailMode
	}{
		{
			t:         BadEncoding,
			pstart:    epoch, // method uses time.Now()
			eventTime: json.Number("0"),
			uuid:      "error",
			clientIP:  "",
			eventName: "Unknown",
			edgeType:  spade.EXTERNAL_EDGE,
			rawProps:  json.RawMessage{},
			failMode:  reporter.FailedTransport,
		},
		{
			t:         Panic,
			pstart:    receivedAt,
			eventTime: json.Number("0"),
			uuid:      "error",
			clientIP:  "",
			eventName: "Unknown",
			edgeType:  spade.INTERNAL_EDGE,
			rawProps:  json.RawMessage([]byte{}),
			failMode:  reporter.PanickedInProcessing,
		},
		{
			t:         Error,
			pstart:    receivedAt,
			eventTime: json.Number(when),
			uuid:      uuid,
			clientIP:  "",
			eventName: "Unknown",
			edgeType:  spade.INTERNAL_EDGE,
			rawProps:  json.RawMessage{},
			failMode:  reporter.UnableToParseData,
		},
		{
			t:         ErrorLongUUID,
			pstart:    receivedAt,
			eventTime: json.Number(when),
			uuid:      "error",
			clientIP:  "",
			eventName: "Unknown",
			edgeType:  spade.INTERNAL_EDGE,
			rawProps:  json.RawMessage{},
			failMode:  reporter.UnableToParseData,
		},
		{
			t:         ErrorEmptyUUID,
			pstart:    receivedAt,
			eventTime: json.Number(when),
			uuid:      "error",
			clientIP:  "",
			eventName: "Unknown",
			edgeType:  spade.INTERNAL_EDGE,
			rawProps:  json.RawMessage{},
			failMode:  reporter.UnableToParseData,
		},
		{
			t:         ErrorEmptyTime,
			pstart:    receivedAt,
			eventTime: json.Number("0"),
			uuid:      uuid,
			clientIP:  "",
			eventName: "Unknown",
			edgeType:  spade.INTERNAL_EDGE,
			rawProps:  json.RawMessage{},
			failMode:  reporter.UnableToParseData,
		},
		{
			t:         ErrorInvalidTime,
			pstart:    receivedAt,
			eventTime: json.Number("0"),
			uuid:      uuid,
			clientIP:  "",
			eventName: "Unknown",
			edgeType:  spade.INTERNAL_EDGE,
			rawProps:  json.RawMessage{},
			failMode:  reporter.UnableToParseData,
		},
		{
			t:         ErrorInvalidEdgeType,
			pstart:    receivedAt,
			eventTime: json.Number(when),
			uuid:      uuid,
			clientIP:  "",
			eventName: "Unknown",
			edgeType:  unknownEdgeType,
			rawProps:  json.RawMessage{},
			failMode:  reporter.UnableToParseData,
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
		if tt.clientIP != me.ClientIP {
			t.Fatalf("mixpanelevent: Expected ClientIP of %s, got %s", tt.clientIP, me.ClientIP)
		}
		if tt.eventName != me.Event {
			t.Fatalf("mixpanelevent: Expected Event of %s, got %s", tt.eventName, me.Event)
		}
		if tt.edgeType != me.EdgeType {
			t.Fatalf("mixpanelevent: Expected EdgeType of %s, got %s", tt.edgeType, me.EdgeType)
		}
		if !reflect.DeepEqual(tt.rawProps, me.Properties) {
			t.Fatalf("mixpanelevent: Expected Properties of %v, got %v", tt.rawProps, me.Properties)
		}
		if tt.failMode != me.Failure {
			t.Fatalf("mixpanelevent: Expected Failure of %s, got %s", tt.failMode, me.Failure)
		}
	}
}

func makeBadEncodedEvent() *MixpanelEvent {
	return &MixpanelEvent{
		Pstart:     time.Now(),
		EventTime:  json.Number("0"),
		UUID:       "error",
		ClientIP:   "",
		Event:      "Unknown",
		EdgeType:   spade.EXTERNAL_EDGE,
		Properties: json.RawMessage{},
		Failure:    reporter.FailedTransport,
	}
}
