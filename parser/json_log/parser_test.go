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
		t.Fatalf("loadFile: %v", path, err)
	}
	return b
}

func TestSuccessfulJsonLogParser(t *testing.T) {
	uuid := "123abc"
	data := string(loadFile("../test_resources/b64payload.txt", t))

	testProperties := json.RawMessage(loadFile("../test_resources/properties.json", t))

	jsonParser := &jsonLogParser{}

	b, err := spade.Marshal(spade.NewEvent(receivedAt, net.IPv4(10, 0, 0, 1), uuid, data))
	if err != nil {
		t.Fatalf("create event: unexpected error %v", err)
	}
	l := &line{b}

	mes, err := jsonParser.Parse(l)
	if err != nil {
		t.Fatalf("parsing: unexpected error %v", err)
	}
	if len(mes) != 1 {
		t.Fatalf("parsing: unexpected number of events. Expected %d got %d", 1, len(mes))
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
	if me.UUID != uuid {
		t.Fatalf("mixpanel event: incorrect uuid. Expected %v got %v", uuid, me.UUID)
	}
	if me.ClientIp != net.IPv4(10, 0, 0, 1).String() {
		t.Fatalf("mixpanel event: incorrect clientIp. Expected %v got %v", net.IPv4(10, 0, 0, 1).String(), me.ClientIp)
	}
	if me.Event != "login" {
		t.Fatalf("mixpanel event: incorrect event name. Expected %v got %v", "login", me.Event)
	}
	if !reflect.DeepEqual(me.Properties, testProperties) {
		t.Fatalf("mixpanel event: mismatched properties. Expected %t got %t", testProperties, me.Properties)
	}
}

func TestFailurePaths(t *testing.T) {
	tests := []struct {
		logLine         []byte
		expectedUuid    string
		expectedTime    json.Number
		expectedFailure reporter.FailMode
	}{
		{ // Incomplete Json
			logLine:         []byte(`{`),
			expectedUuid:    "error",
			expectedTime:    json.Number("0"),
			expectedFailure: reporter.UNABLE_TO_PARSE_DATA,
		},
		{ // Incorrect base64 data
			logLine:         loadFile("test_resources/brokendata.json", t),
			expectedUuid:    "123abc",
			expectedTime:    json.Number("1418172623"),
			expectedFailure: reporter.UNABLE_TO_PARSE_DATA,
		},
	}

	jsonParser := &jsonLogParser{}
	for _, tt := range tests {
		mes, err := jsonParser.Parse(&line{tt.logLine})
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
