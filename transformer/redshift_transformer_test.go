package transformer

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/cactus/go-statsd-client/statsd"

	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/lookup"
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/writer"
)

// idFetcherMock implements a dummy RedshiftValueFetcher that just checks if you're asking
// for an int64 with an expected key, otherwise it returns an error.
type idFetcherMock struct {
	expectedLogin string
}

func (f *idFetcherMock) Fetch(args map[string]string) (interface{}, error) {
	return nil, errors.New("Not expected to fetch for an interface")
}

func (f *idFetcherMock) FetchInt64(args map[string]string) (int64, error) {
	login, ok := args["login"]
	if !ok {
		return 0, fmt.Errorf("Can't fetch login from args. Received: %v", args)
	}
	if login == "errExtractingValue" {
		return 0, lookup.ErrExtractingValue
	}
	if login != f.expectedLogin {
		return 0, fmt.Errorf("Can't fetch a value with login '%v', it should be %v ", login,
			f.expectedLogin)
	}
	return 42, nil
}

// cacheMock implements a TransformerCache that always fails in Get and does nothing in Set.
type cacheMock struct{}

func (c *cacheMock) Get(key string) (string, error) {
	return "", errors.New("memcache: cache miss")
}

func (c *cacheMock) Set(key string, value string) error {
	return nil
}

type statsMock struct{}

func (s *statsMock) Timing(stat string, t time.Duration) {
}

func (s *statsMock) IncrBy(stat string, value int) {
}

func (s *statsMock) GetStatter() statsd.Statter {
	return nil
}

// This tests that the logic is implemented correctly to
// transform a parsed event to some table psql format.
// Thus this does not test the redsshift types.
// Modes to test:
//  - Normal Mode: event conforms and is transformed successfully
//  - Normal Mode with mapping: event conforms and is transformed and mapped successfully
//  - Not tracked Mode: there is no table for this event
//  - Empty Event: no text associated with this event
//  - Transform error: Event contains a column that does not convert.
//  - No mapping event: Event doesn't contain the required mapping columns.
//  - Bad Parse event: Event already contains an error
type testLoader struct {
	Configs  map[string][]RedshiftType
	Versions map[string]int
}

type testEventMetadataLoader struct {
	configs map[string](map[string]string)
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

func (s *testEventMetadataLoader) GetMetadataValueByType(eventName string, metadataType string) (string, error) {
	if eventMetadata, found := s.configs[eventName]; found {
		if metadata, exists := eventMetadata[metadataType]; exists {
			return metadata, nil
		}
		return "", nil
	}
	return "", nil
	// return "", ErrNotTracked{
	// 	What: fmt.Sprintf("%s is not being tracked", eventName),
	// }
}

func transformerRunner(t *testing.T, input *parser.MixpanelEvent, expected *writer.WriteRequest) {
	log.SetOutput(bytes.NewBuffer(make([]byte, 0, 256))) // silence log output
	tConfig := MappingTransformerConfig{
		&idFetcherMock{"kai.hayashi"}, &cacheMock{}, &cacheMock{}, &statsMock{}}
	config := &testLoader{
		Configs: map[string][]RedshiftType{
			"login": {
				{intFormat(64), "times", "times", nil},
				{floatFormat, "fraction", "fraction", nil},
				{varcharFormat, "name", "name", nil},
				{varcharFormat, "user_agent", "user_agent", nil},
				{genUnixTimeFormat(PST), "now", "now", nil},
				{genLoginToIDTransformer(tConfig), "id", "id", []string{"name"}},
			},
		},
		Versions: map[string]int{
			"login": 42,
		},
	}
	eventMetadataConfig := &testEventMetadataLoader{
		configs: map[string](map[string]string){
			"test-event": map[string]string{
				"edge_type": "internal",
				"comment":   "test comment",
			},
			"login": map[string]string{},
		},
	}
	_stats, _ := statsd.NewNoop()
	_transformer := NewRedshiftTransformer(config, eventMetadataConfig, reporter.WrapCactusStatter(_stats, 0.1))
	if !reflect.DeepEqual(_transformer.Consume(input), expected) {
		t.Logf("Got \n%v \nexpected \n%v\n", *_transformer.Consume(input), expected)
		t.Logf("Transformer output: %#v", _transformer.Consume(input).Record)
		t.Fail()
	}
}

func TestBadParseEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	badParseEvent := &parser.MixpanelEvent{
		Event:    "login",
		EdgeType: spade.INTERNAL_EDGE,
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155,
			"id":       42}`),
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
		Event:    "login",
		EdgeType: spade.INTERNAL_EDGE,
		Properties: []byte(`{
			"times":    "sda",
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155,
			"id":       42}`),
		UserAgent: "Test Browser",
		Failure:   reporter.None,
		Pstart:    now,
		UUID:      "uuid1",
	}
	expected := writer.WriteRequest{
		Category: "login",
		Version:  42,
		Line:     "\"\"\t\"0.1234\"\t\"kai.hayashi\"\t\"Test Browser\"\t\"2013-10-17 11:05:55\"\t\"42\"",
		Record:   map[string]string{"times": "", "fraction": "0.1234", "name": "kai.hayashi", "now": "2013-10-17 11:05:55", "id": "42", "user_agent": "Test Browser"},
		Source:   transformErrorEvent.Properties,
		Failure:  reporter.SkippedColumn,
		Pstart:   now,
		UUID:     "uuid1",
	}
	transformerRunner(t, transformErrorEvent, &expected)
}

func TestEmptyEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	emptyEvent := &parser.MixpanelEvent{
		Event:    "",
		EdgeType: spade.INTERNAL_EDGE,
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155,
			"id":       42}`),
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
		Event:    "login",
		EdgeType: spade.INTERNAL_EDGE,
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
		Line:     "\"42\"\t\"0.1234\"\t\"kai.hayashi\"\t\"\"\t\"\"\t\"42\"",
		Record:   map[string]string{"times": "42", "fraction": "0.1234", "name": "kai.hayashi", "id": "42", "": ""},
		Source:   emptyEvent.Properties,
		Failure:  reporter.SkippedColumn,
		Pstart:   now,
		UUID:     "uuid1",
	}
	transformerRunner(t, emptyEvent, &expected)
}

func TestNotTrackedEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	notTrackedEvent := &parser.MixpanelEvent{
		Event:    "NotTracked",
		EdgeType: spade.INTERNAL_EDGE,
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155,
			"id":       42
		}`),
		Failure: reporter.None,
		Pstart:  now,
		UUID:    "uuid1",
	}
	transformerRunner(t, notTrackedEvent, &writer.WriteRequest{
		Category: notTrackedEvent.Event,
		Line:     `{"event":"NotTracked","properties":{"times":42,"fraction":0.1234,"name":"kai.hayashi","now":1382033155,"id":42}}`,
		Source:   notTrackedEvent.Properties,
		Failure:  reporter.NonTrackingEvent,
		Pstart:   notTrackedEvent.Pstart,
		UUID:     "uuid1",
	})
}

func TestEventWithNoMappingConsume(t *testing.T) {
	now := time.Now().In(PST)
	noMappingEvent := &parser.MixpanelEvent{
		Event:    "login",
		EdgeType: spade.INTERNAL_EDGE,
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"now":      1382033155,
			"id":       null}
		}`),
		Failure: reporter.None,
		Pstart:  now,
		UUID:    "uuid1",
	}
	expected := writer.WriteRequest{
		Category: "login",
		Version:  42,
		Line:     "\"42\"\t\"0.1234\"\t\"\"\t\"\"\t\"2013-10-17 11:05:55\"\t\"\"",
		Record:   map[string]string{"times": "42", "fraction": "0.1234", "now": "2013-10-17 11:05:55", "id": "", "": ""},
		Source:   noMappingEvent.Properties,
		UUID:     "uuid1",
		Failure:  reporter.SkippedColumn,
		Pstart:   now,
	}
	transformerRunner(t, noMappingEvent, &expected)
}

func TestNormalEventConsume(t *testing.T) {
	now := time.Now().In(PST)
	normalEvent := &parser.MixpanelEvent{
		Event:    "login",
		EdgeType: spade.INTERNAL_EDGE,
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155,
			"id":       42}
		}`),
		UserAgent: "Test Browser",
		Failure:   reporter.None,
		Pstart:    now,
		UUID:      "uuid1",
	}
	expected := writer.WriteRequest{
		Category: "login",
		Version:  42,
		Line:     "\"42\"\t\"0.1234\"\t\"kai.hayashi\"\t\"Test Browser\"\t\"2013-10-17 11:05:55\"\t\"42\"",
		Record:   map[string]string{"times": "42", "fraction": "0.1234", "name": "kai.hayashi", "now": "2013-10-17 11:05:55", "id": "42", "user_agent": "Test Browser"},
		Source:   normalEvent.Properties,
		UUID:     "uuid1",
		Failure:  reporter.None,
		Pstart:   now,
	}
	transformerRunner(t, normalEvent, &expected)
}

func TestNormalEventWithMappingConsume(t *testing.T) {
	now := time.Now().In(PST)
	normalEvent := &parser.MixpanelEvent{
		Event:    "login",
		EdgeType: spade.INTERNAL_EDGE,
		Properties: []byte(`{
			"times":    42,
			"fraction": 0.1234,
			"name":     "kai.hayashi",
			"now":      1382033155,
			"id":       null}
		}`),
		UserAgent: "Test Browser",
		Failure:   reporter.None,
		Pstart:    now,
		UUID:      "uuid1",
	}
	expected := writer.WriteRequest{
		Category: "login",
		Version:  42,
		Line:     "\"42\"\t\"0.1234\"\t\"kai.hayashi\"\t\"Test Browser\"\t\"2013-10-17 11:05:55\"\t\"42\"",
		Record:   map[string]string{"times": "42", "fraction": "0.1234", "name": "kai.hayashi", "now": "2013-10-17 11:05:55", "id": "42", "user_agent": "Test Browser"},
		Source:   normalEvent.Properties,
		UUID:     "uuid1",
		Failure:  reporter.None,
		Pstart:   now,
	}
	transformerRunner(t, normalEvent, &expected)
}
