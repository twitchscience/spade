package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
)

var receivedAt = time.Now()

type line struct {
	b []byte
}

func (i *line) Data() []byte {
	return i.b
}

func (i *line) StartTime() time.Time {
	return receivedAt
}

func loadFile(path string, t *testing.T) []byte {
	fd, err := os.Open(path)
	if err != nil {
		t.Fatalf("loadFile: %v", err)
	}
	b, err := ioutil.ReadAll(fd)
	if err != nil {
		t.Fatalf("loadFile: %s, %v", path, err)
	}
	return bytes.Trim(b, "\n")
}

func singleEventUUIDChecker(mes []parser.MixpanelEvent, t *testing.T, uuid string) {
	if mes[0].UUID != uuid {
		t.Fatalf("mixpanel event: incorrect uuid. Expected %s got %s", uuid, mes[0].UUID)
	}
}

func multipleEventUUIDChecker(mes []parser.MixpanelEvent, t *testing.T, uuidStub string) {
	for i, me := range mes {
		expectedUUID := fmt.Sprintf("%s-%d", uuidStub, i)
		if me.UUID != expectedUUID {
			t.Fatalf("mixpanel event: incorrect multievent uuid. Expected %s got %s", expectedUUID, me.UUID)
		}
	}
}

func TestSuccessfulJSONLogParser(t *testing.T) {
	tests := []struct {
		logLine        string
		rawProps       json.RawMessage
		expectedEvents int
		uuidChecker    func([]parser.MixpanelEvent, *testing.T, string)
	}{
		{
			logLine:        string(loadFile("../test_resources/b64payload.txt", t)),
			rawProps:       json.RawMessage(loadFile("../test_resources/properties.json", t)),
			expectedEvents: 1,
			uuidChecker:    singleEventUUIDChecker,
		},
		{
			logLine:        string(loadFile("../test_resources/multievent_b64.txt", t)),
			rawProps:       json.RawMessage(loadFile("../test_resources/properties.json", t)),
			expectedEvents: 2,
			uuidChecker:    multipleEventUUIDChecker,
		},
	}

	jsonParser := &LogParser{}
	uuid := "123abc"
	userAgent := "TestBrowser"
	for _, tt := range tests {
		b, err := spade.Marshal(spade.NewEvent(receivedAt, net.IPv4(10, 0, 0, 1), "10.0.0.1", uuid,
			tt.logLine, userAgent, spade.INTERNAL_EDGE))
		if err != nil {
			t.Fatalf("create event: unexpected error %v", err)
		}
		l := &line{b}

		mes, err := jsonParser.Parse(l)
		if err != nil {
			t.Fatalf("parsing: unexpected error %v", err)
		}
		if len(mes) != tt.expectedEvents {
			t.Fatalf("parsing: unexpected number of events. Expected %d got %d", tt.expectedEvents, len(mes))
		}

		me := mes[0]
		if me.Pstart != receivedAt {
			t.Fatalf("mixpanel event: incorrect Pstart times. Expected %v got %v", receivedAt, me.Pstart)
		}
		if me.EventTime != receivedAt {
			t.Fatalf("mixpanel event: incorrect event times. Expected %s got %s", receivedAt, me.EventTime)
		}
		if me.ClientIP != net.IPv4(10, 0, 0, 1).String() {
			t.Fatalf("mixpanel event: incorrect clientIP. Expected %v got %v", net.IPv4(10, 0, 0, 1).String(), me.ClientIP)
		}
		if me.Event != "login" {
			t.Fatalf("mixpanel event: incorrect event name. Expected %v got %v", "login", me.Event)
		}
		if me.UserAgent != userAgent {
			t.Fatalf("mixpanel event: incorrect user agent. Expected %v got %v", userAgent, me.UserAgent)
		}
		assert.Equal(t, string(tt.rawProps), string(me.Properties))

		tt.uuidChecker(mes, t, uuid)
	}

}

func TestFailurePaths(t *testing.T) {
	tests := []struct {
		errorName       string
		logLine         []byte
		expectedUUID    string
		expectedTime    time.Time
		expectedFailure reporter.FailMode
	}{
		{
			errorName:       "Incomplete Json",
			logLine:         []byte(`{`),
			expectedUUID:    "error",
			expectedTime:    time.Time{},
			expectedFailure: reporter.UnableToParseData,
		},
		{
			errorName:       "Incorrect base64 data",
			logLine:         loadFile("test_resources/broken_b64_data.json", t),
			expectedUUID:    "123abc",
			expectedTime:    time.Unix(1418172623, 0).UTC(),
			expectedFailure: reporter.UnableToParseData,
		},
	}

	jsonParser := &LogParser{}
	for _, tt := range tests {
		mes, err := jsonParser.Parse(&line{tt.logLine})
		if err == nil {
			t.Fatalf("parsing: expected failure. Failing test name: %s", tt.errorName)
		}
		if len(mes) == 0 {
			t.Fatalf("parsing: expected mixpanel events. Failing test name: %s", tt.errorName)
		}
		if mes[0].Failure != tt.expectedFailure {
			t.Fatalf("mixpanel event: incorrect failure mode. Expected %s got %s. Failing test name: %s", tt.expectedFailure, mes[0].Failure, tt.errorName)
		}
		if mes[0].UUID != tt.expectedUUID {
			t.Fatalf("mixpanel event: incorrect uuid. Expected %s got %s. Failing test name: %s", tt.expectedUUID, mes[0].UUID, tt.errorName)
		}
		if mes[0].EventTime != tt.expectedTime {
			t.Fatalf("mixpanel event: incorrect time. Expected %s got %s. Failing test name: %s", tt.expectedTime, mes[0].EventTime, tt.errorName)
		}
	}
}
