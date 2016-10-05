package transformer

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/writer"

	"log"
	"testing"
	"time"
)

// This tests that the logic is implemented correctly to
// transform a parsed event to some table psql format.
// Thus this does not test the redsshift types.
// Modes to test:
//  - Normal Mode: event conforms and is transformed succesfully
//  - Not tracked Mode: there is no table for this event
//  - Empty Event: no text associated with this event
//  - Transform error: Event contains a column that does not convert.
//  - Bad Parse event: Event already contains an error
type testLoader struct {
	Configs  map[string][]RedshiftType
	Versions map[string]int
}

func (s *testLoader) Refresh() error {
	return nil
}

func (s *testLoader) GetColumnsForEvent(eventName string) ([]RedshiftType, error) {
	if transformArray, exists := s.Configs[eventName]; exists {
		return transformArray, nil
	}
	return nil, ErrNotTracked{fmt.Sprintf("%s is not being tracked", eventName)}
}
func (s *testLoader) GetVersionForEvent(eventName string) int {
	if version, exists := s.Versions[eventName]; exists {
		return version
	}
	return 0
}

func transformerRunner(t *testing.T, input *parser.MixpanelEvent, expected *writer.WriteRequest) {
	log.SetOutput(bytes.NewBuffer(make([]byte, 0, 256))) // silence log output
	config := &testLoader{
		Configs: map[string][]RedshiftType{
			"login": {
				{intFormat(64), "times", "times"},
				{floatFormat, "fraction", "fraction"},
				{varcharFormat, "name", "name"},
				{genUnixTimeFormat(PST), "now", "now"},
			},
		},
		Versions: map[string]int{
			"login": 42,
		},
	}
	_transformer := NewRedshiftTransformer(config)
	if !reflect.DeepEqual(_transformer.Consume(input), expected) {
		t.Logf("Got %v expected %v\n", *_transformer.Consume(input), expected)
		t.Logf("%#v", _transformer.Consume(input).Record)
		t.Fail()
	}
}

func TestBadParseEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	badParseEvent := &parser.MixpanelEvent{
		Event: "login",
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155}`),
		Failure: reporter.UnableToParseData,
		Pstart:  now,
		UUID:    "uuid1",
	}
	transformerRunner(t, badParseEvent, &writer.WriteRequest{
		Category: badParseEvent.Event,
		Version:  42,
		Line:     "",
		UUID:     "uuid1",
		Source:   badParseEvent.Properties,
		Failure:  reporter.UnableToParseData,
		Pstart:   badParseEvent.Pstart,
	})
}

func TestTransformBadColumnEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	transformErrorEvent := &parser.MixpanelEvent{
		Event: "login",
		Properties: []byte(`{
			"times":    "sda",
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155}`),
		Failure: reporter.None,
		Pstart:  now,
		UUID:    "uuid1",
	}
	expected := writer.WriteRequest{
		Category: "login",
		Version:  42,
		Line:     "\"\"\t\"0.1234\"\t\"kai.hayashi\"\t\"2013-10-17 11:05:55\"",
		Record:   map[string]string{"times": "", "fraction": "0.1234", "name": "kai.hayashi", "now": "2013-10-17 11:05:55"},
		Source: []byte(`{
			"times":    "sda",
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155}`),
		Failure: reporter.SkippedColumn,
		Pstart:  now,
		UUID:    "uuid1",
	}
	transformerRunner(t, transformErrorEvent, &expected)
}

func TestEmptyEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	emptyEvent := &parser.MixpanelEvent{
		Event: "",
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155}`),
		Failure: reporter.None,
		Pstart:  now,
		UUID:    "uuid1",
	}
	transformerRunner(t, emptyEvent, &writer.WriteRequest{
		Category: "Unknown",
		Line:     "",
		Source:   emptyEvent.Properties,
		Failure:  reporter.EmptyRequest,
		Pstart:   emptyEvent.Pstart,
		UUID:     "uuid1",
	})
}

func TestMissingPropertyEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	emptyEvent := &parser.MixpanelEvent{
		Event: "login",
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi"}`),
		Failure: reporter.None,
		Pstart:  now,
		UUID:    "uuid1",
	}
	expected := writer.WriteRequest{
		Category: "login",
		Version:  42,
		Line:     "\"42\"\t\"0.1234\"\t\"kai.hayashi\"\t\"\"",
		Record:   map[string]string{"times": "42", "fraction": "0.1234", "name": "kai.hayashi", "": ""},
		Source: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi"}`),
		Failure: reporter.SkippedColumn,
		Pstart:  now,
		UUID:    "uuid1",
	}
	transformerRunner(t, emptyEvent, &expected)
}

func TestNotTrackedEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	notTrackedEvent := &parser.MixpanelEvent{
		Event: "NotTracked",
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155
		}`),
		Failure: reporter.None,
		Pstart:  now,
		UUID:    "uuid1",
	}
	transformerRunner(t, notTrackedEvent, &writer.WriteRequest{
		Category: notTrackedEvent.Event,
		Line:     `{"event":"NotTracked","properties":{"times":42,"fraction":0.1234,"name":"kai.hayashi","now":1382033155}}`,
		Source:   notTrackedEvent.Properties,
		Failure:  reporter.NonTrackingEvent,
		Pstart:   notTrackedEvent.Pstart,
		UUID:     "uuid1",
	})
}

func TestNormalEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	normalEvent := &parser.MixpanelEvent{
		Event: "login",
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155
		}`),
		Failure: reporter.None,
		Pstart:  now,
		UUID:    "uuid1",
	}
	expected := writer.WriteRequest{
		Category: "login",
		Version:  42,
		Line:     "\"42\"\t\"0.1234\"\t\"kai.hayashi\"\t\"2013-10-17 11:05:55\"",
		Record:   map[string]string{"times": "42", "fraction": "0.1234", "name": "kai.hayashi", "now": "2013-10-17 11:05:55"},
		Source: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155
		}`),
		UUID:    "uuid1",
		Failure: reporter.None,
		Pstart:  now,
	}
	transformerRunner(t, normalEvent, &expected)
}
