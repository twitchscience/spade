package json_log

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

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
	return b
}

func singleEventUuidChecker(mes []parser.MixpanelEvent, t *testing.T, uuid string) {
	if mes[0].UUID != uuid {
		t.Fatalf("mixpanel event: incorrect uuid. Expected %s got %s", uuid, mes[0].UUID)
	}
}

func multipleEventUuidChecker(mes []parser.MixpanelEvent, t *testing.T, uuidStub string) {
	for i, me := range mes {
		expectedUuid := fmt.Sprintf("%s-%d", uuidStub, i)
		if me.UUID != expectedUuid {
			t.Fatalf("mixpanel event: incorrect multievent uuid. Expected %s got %s", expectedUuid, me.UUID)
		}
	}
}

func TestSuccessfulJsonLogParser(t *testing.T) {
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
			uuidChecker:    singleEventUuidChecker,
		},
		{
			logLine:        string(loadFile("../test_resources/multievent_b64.txt", t)),
			rawProps:       json.RawMessage(loadFile("../test_resources/properties.json", t)),
			expectedEvents: 2,
			uuidChecker:    multipleEventUuidChecker,
		},
	}

	jsonParser := &jsonLogParser{}
	uuid := "123abc"
	for _, tt := range tests {
		b, err := spade.Marshal(spade.NewEvent(receivedAt, net.IPv4(10, 0, 0, 1), "10.0.0.1", uuid, tt.logLine))
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
		if me.EventTime != json.Number(fmt.Sprintf("%d", receivedAt.Unix())) {
			t.Fatalf(
				"mixpanel event: incorrect event times. Expected %s got %s",
				json.Number(fmt.Sprintf("%d", receivedAt.Unix())),
				me.EventTime,
			)
		}
		if me.ClientIp != net.IPv4(10, 0, 0, 1).String() {
			t.Fatalf("mixpanel event: incorrect clientIp. Expected %v got %v", net.IPv4(10, 0, 0, 1).String(), me.ClientIp)
		}
		if me.Event != "login" {
			t.Fatalf("mixpanel event: incorrect event name. Expected %v got %v", "login", me.Event)
		}
		if !reflect.DeepEqual(me.Properties, tt.rawProps) {
			t.Fatalf("mixpanel event: mismatched properties. Expected %s got %s", tt.rawProps, me.Properties)
		}

		tt.uuidChecker(mes, t, uuid)
	}

}

func TestFailurePaths(t *testing.T) {
	tests := []struct {
		errorName       string
		logLine         []byte
		expectedUuid    string
		expectedTime    json.Number
		expectedFailure reporter.FailMode
	}{
		{
			errorName:       "Incomplete Json",
			logLine:         []byte(`{`),
			expectedUuid:    "error",
			expectedTime:    json.Number("0"),
			expectedFailure: reporter.UNABLE_TO_PARSE_DATA,
		},
		{
			errorName:       "Incorrect base64 data",
			logLine:         loadFile("test_resources/broken_b64_data.json", t),
			expectedUuid:    "123abc",
			expectedTime:    json.Number("1418172623"),
			expectedFailure: reporter.UNABLE_TO_PARSE_DATA,
		},
	}

	jsonParser := &jsonLogParser{}
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
		if mes[0].UUID != tt.expectedUuid {
			t.Fatalf("mixpanel event: incorrect uuid. Expected %s got %s. Failing test name: %s", tt.expectedUuid, mes[0].UUID, tt.errorName)
		}
		if mes[0].EventTime != tt.expectedTime {
			t.Fatalf("mixpanel event: incorrect time. Expected %s got %s. Failing test name: %s", tt.expectedTime, mes[0].EventTime, tt.errorName)
		}
	}
}

func TestIPFailures(t *testing.T) {
	tests := []struct {
		errorName          string
		logLine            []byte
		expectedUuid       string
		expectedTime       json.Number
		expectedFailure    reporter.FailMode
		rejectIfBadFirstIp bool
		noFail             bool
	}{
		{
			errorName:          "Reject row for bad IP under previous definition",
			logLine:            loadFile("test_resources/broken_ip_data.json", t),
			expectedUuid:       "123abc",
			expectedTime:       json.Number("1418172623"),
			expectedFailure:    reporter.UNABLE_TO_PARSE_DATA,
			rejectIfBadFirstIp: true,
		},
		{
			errorName:       "Don't reject a row for a bad IP when the bug is disabled",
			logLine:         loadFile("test_resources/broken_ip_data.json", t),
			expectedUuid:    "123abc",
			expectedTime:    json.Number("1418172623"),
			expectedFailure: reporter.NONE,
			noFail:          true,
		},
	}

	for _, tt := range tests {
		jsonParser := &jsonLogParser{tt.rejectIfBadFirstIp}
		mes, err := jsonParser.Parse(&line{tt.logLine})
		if err == nil && !tt.noFail {
			t.Fatalf("parsing: expected failure. Failing test name: %s", tt.errorName)
		}
		if len(mes) == 0 {
			t.Fatalf("parsing: expected mixpanel events. Failing test name: %s", tt.errorName)
		}
		if mes[0].Failure != tt.expectedFailure {
			t.Fatalf("mixpanel event: incorrect failure mode. Expected %s got %s. Failing test name: %s", tt.expectedFailure, mes[0].Failure, tt.errorName)
		}
		if mes[0].UUID != tt.expectedUuid {
			t.Fatalf("mixpanel event: incorrect uuid. Expected %s got %s. Failing test name: %s", tt.expectedUuid, mes[0].UUID, tt.errorName)
		}
		if mes[0].EventTime != tt.expectedTime {
			t.Fatalf("mixpanel event: incorrect time. Expected %s got %s. Failing test name: %s", tt.expectedTime, mes[0].EventTime, tt.errorName)
		}
	}
}
