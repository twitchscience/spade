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
	Configs map[string][]RedshiftType
}

func (s *testLoader) Refresh() error {
	return nil
}

func (s *testLoader) GetColumnsForEvent(eventName string) ([]RedshiftType, error) {
	if transformArray, exists := s.Configs[eventName]; exists {
		return transformArray, nil
	}
	return nil, NotTrackedError{fmt.Sprintf("%s is not being tracked", eventName)}
}

func _transformer_runner(t *testing.T, input *parser.MixpanelEvent, expected *writer.WriteRequest) {
	log.SetOutput(bytes.NewBuffer(make([]byte, 0, 256))) // silence log output
	var _config = &testLoader{
		Configs: map[string][]RedshiftType{
			"login": []RedshiftType{
				RedshiftType{intFormat(64), "times"},
				RedshiftType{floatFormat, "fraction"},
				RedshiftType{varcharFormat, "name"},
				RedshiftType{unixTimeFormat, "now"},
			},
		},
	}
	var _transformer Transformer = NewRedshiftTransformer(_config)
	if !reflect.DeepEqual(_transformer.Consume(input), expected) {
		t.Logf("Got %v expected %v\n", *_transformer.Consume(input), expected)
		t.Fail()
	}
}

func TestBadParseEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	var _badParseEvent *parser.MixpanelEvent = &parser.MixpanelEvent{
		Event: "login",
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155}`),
		Failure: reporter.UNABLE_TO_PARSE_DATA,
		Pstart:  now,
		UUID:    "uuid1",
	}
	_transformer_runner(t, _badParseEvent, &writer.WriteRequest{
		Category: _badParseEvent.Event,
		Line:     "",
		UUID:     "uuid1",
		Source:   _badParseEvent.Properties,
		Failure:  reporter.UNABLE_TO_PARSE_DATA,
		Pstart:   _badParseEvent.Pstart,
	})
}

func TestTransformBadColumnEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	var _transformErrorEvent *parser.MixpanelEvent = &parser.MixpanelEvent{
		Event: "login",
		Properties: []byte(`{
			"times":    "sda",
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155}`),
		Failure: reporter.NONE,
		Pstart:  now,
		UUID:    "uuid1",
	}
	expected := writer.WriteRequest{
		Category: "login",
		Line:     "\t0.1234\t\"kai.hayashi\"\t2013-10-17 11:05:55",
		Source: []byte(`{
			"times":    "sda",
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155}`),
		Failure: reporter.SKIPPED_COLUMN,
		Pstart:  now,
		UUID:    "uuid1",
	}
	_transformer_runner(t, _transformErrorEvent, &expected)
}

func TestEmptyEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	var _emptyEvent *parser.MixpanelEvent = &parser.MixpanelEvent{
		Event: "",
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155}`),
		Failure: reporter.NONE,
		Pstart:  now,
		UUID:    "uuid1",
	}
	_transformer_runner(t, _emptyEvent, &writer.WriteRequest{
		Category: "Unknown",
		Line:     "",
		Source:   _emptyEvent.Properties,
		Failure:  reporter.EMPTY_REQUEST,
		Pstart:   _emptyEvent.Pstart,
		UUID:     "uuid1",
	})
}

func TestMissingPropertyEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	var _emptyEvent *parser.MixpanelEvent = &parser.MixpanelEvent{
		Event: "login",
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi"}`),
		Failure: reporter.NONE,
		Pstart:  now,
		UUID:    "uuid1",
	}
	expected := writer.WriteRequest{
		Category: "login",
		Line:     "42\t0.1234\t\"kai.hayashi\"\t",
		Source: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi"}`),
		Failure: reporter.SKIPPED_COLUMN,
		Pstart:  now,
		UUID:    "uuid1",
	}
	_transformer_runner(t, _emptyEvent, &expected)
}

func TestNotTrackedEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	var _notTrackedEvent *parser.MixpanelEvent = &parser.MixpanelEvent{
		Event: "NotTracked",
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155
		}`),
		Failure: reporter.NONE,
		Pstart:  now,
		UUID:    "uuid1",
	}
	_transformer_runner(t, _notTrackedEvent, &writer.WriteRequest{
		Category: _notTrackedEvent.Event,
		Line:     `{"event":"NotTracked","properties":{"times":42,"fraction":0.1234,"name":"kai.hayashi","now":1382033155}}`,
		Source:   _notTrackedEvent.Properties,
		Failure:  reporter.NON_TRACKING_EVENT,
		Pstart:   _notTrackedEvent.Pstart,
		UUID:     "uuid1",
	})
}

func TestNormalEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	var _normalEvent *parser.MixpanelEvent = &parser.MixpanelEvent{
		Event: "login",
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155
		}`),
		Failure: reporter.NONE,
		Pstart:  now,
		UUID:    "uuid1",
	}
	expected := writer.WriteRequest{
		Category: "login",
		Line:     "42\t0.1234\t\"kai.hayashi\"\t2013-10-17 11:05:55",
		Source: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155
		}`),
		UUID:    "uuid1",
		Failure: reporter.NONE,
		Pstart:  now,
	}
	_transformer_runner(t, _normalEvent, &expected)
}
