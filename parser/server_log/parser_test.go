package server_log

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
)

var receivedAt = time.Unix(1418172623, 0)

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
		t.Fatalf("loadFile: %v", path, err)
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

func TestSuccessfulServerLogParser(t *testing.T) {
	tests := []struct {
		data           string
		rawProps       json.RawMessage
		expectedEvents int
		uuidChecker    func([]parser.MixpanelEvent, *testing.T, string)
	}{
		{
			data:           string(loadFile("../test_resources/b64payload.txt", t)),
			rawProps:       json.RawMessage(loadFile("../test_resources/properties.json", t)),
			expectedEvents: 1,
			uuidChecker:    singleEventUuidChecker,
		},
		{
			data:           string(loadFile("../test_resources/multievent_b64.txt", t)),
			rawProps:       json.RawMessage(loadFile("../test_resources/properties.json", t)),
			expectedEvents: 2,
			uuidChecker:    multipleEventUuidChecker,
		},
	}

	serverParser := &serverLogParser{
		escaper: &parser.ByteQueryUnescaper{},
	}
	uuid := "123abc"
	for _, tt := range tests {
		serverLogLine := fmt.Sprintf(
			"10.0.0.1/- [%s.000] data=%s %s recordversion=1",
			"1418172623",
			tt.data,
			uuid,
		)

		l := &line{[]byte(serverLogLine)}
		mes, err := serverParser.Parse(l)
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
				"mixpanel event: incorrect event times. Expected %t got %t",
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
		data            []byte
		expectedUuid    string
		expectedTime    json.Number
		expectedFailure reporter.FailMode
	}{
		{ // Incorrect base64 data
			data:            []byte(`notvalidbase64`),
			expectedUuid:    "123abc",
			expectedTime:    json.Number("1418172623"),
			expectedFailure: reporter.UNABLE_TO_PARSE_DATA,
		},
	}

	serverParser := &serverLogParser{
		escaper: &parser.ByteQueryUnescaper{},
	}
	uuid := "123abc"
	for _, tt := range tests {
		serverLogLine := fmt.Sprintf(
			"10.0.0.1/- [%s.000] data=%s %s recordversion=1",
			"1418172623",
			tt.data,
			uuid,
		)
		l := &line{[]byte(serverLogLine)}

		mes, err := serverParser.Parse(l)
		if err == nil {
			t.Fatalf("parsing: expected failure")
		}
		if len(mes) == 0 {
			t.Fatalf("parsing: expected mixpanel events")
		}
		if mes[0].Failure != tt.expectedFailure {
			t.Fatalf("mixpanel event: incorrect failure mode. Expected %t got %t", tt.expectedFailure, mes[0].Failure)
		}
		if mes[0].UUID != tt.expectedUuid {
			t.Fatalf("mixpanel event: incorrect uuid. Expected %s got %s", tt.expectedUuid, mes[0].UUID)
		}
		if mes[0].EventTime != tt.expectedTime {
			t.Fatalf("mixpanel event: incorrect time. Expected %t got %t", tt.expectedTime, mes[0].EventTime)
		}
	}
}
