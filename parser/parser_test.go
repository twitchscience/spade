package parser

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/twitchscience/spade/reporter"
)

type errorParser struct{}

func (_ *errorParser) Parse(_ Parseable) ([]MixpanelEvent, error) {
	return nil, fmt.Errorf("parser: expected error")
}

type singleEventParser struct{}

func (_ *singleEventParser) Parse(_ Parseable) ([]MixpanelEvent, error) {
	return make([]MixpanelEvent, 1), nil
}

type multiEventParser struct{}

func (_ *multiEventParser) Parse(_ Parseable) ([]MixpanelEvent, error) {
	return make([]MixpanelEvent, 2), nil
}

func TestRegisterAndClearing(t *testing.T) {
	Register("test_parser", &singleEventParser{})
	if _, ok := parsers["test_parser"]; !ok {
		t.Fatalf("register: test_parser not present, expected it to be. Current parsers: %v", parsers)
	}
	clearAll()
	if _, ok := parsers["test_parser"]; ok {
		t.Fatalf("register: test_parser present, expected it not to be. Current parsers: %v", parsers)
	}
}

type testReporter struct {
	cnt int
}

func (t *testReporter) Record(_ *reporter.Result) {}
func (t *testReporter) IncrementExpected(i int) {
	t.cnt += i
}
func (t *testReporter) Reset() {}
func (t *testReporter) Finalize() map[string]int {
	return make(map[string]int, 1)
}

var receivedAt = time.Now()

type logLine struct{}

func (l *logLine) Data() []byte {
	return []byte{}
}

func (l *logLine) StartTime() time.Time {
	return receivedAt
}

func TestNoParserCall(t *testing.T) {
	clearAll() // TODO: move to TestMain() when we move to go 1.4

	fop := BuildSpadeParser(&testReporter{})
	mes, err := fop.Parse(&logLine{})
	if err == nil {
		t.Fatalf("parser: expected error, didn't get one")
	}
	if mes != nil {
		t.Fatal("parser: expected events to be nil, got %v", mes)
	}
}

func TestParseCall(t *testing.T) {
	clearAll() // TODO: move to TestMain() when we move to go 1.4

	tests := []struct {
		parser         Parser
		expectedEvents int
		expectError    bool
	}{
		{parser: &errorParser{}, expectedEvents: 0, expectError: true},
		{parser: &singleEventParser{}, expectedEvents: 1, expectError: false},
		{parser: &multiEventParser{}, expectedEvents: 2, expectError: false},
	}

	tr := &testReporter{}
	fop := BuildSpadeParser(tr)
	for _, tt := range tests {
		clearAll()
		Register("current_parser", tt.parser)
		mes, err := fop.Parse(&logLine{})
		if tt.expectError && err == nil {
			t.Fatalf("parser: expected error, didn't get one from %v", reflect.TypeOf(tt.parser))
		}
		if !tt.expectError && err != nil {
			t.Fatalf("parser: unexpected error: %v", err)
		}
		if len(mes) != tt.expectedEvents {
			t.Fatalf("parser: unexpected number of events. Expected %d, got %d", tt.expectedEvents, len(mes))
		}
		if tt.expectedEvents > 1 && tr.cnt != (tt.expectedEvents-1) {
			t.Fatalf(
				"reporter: expected parser to increment expectation. Expected %d, got %d",
				(tt.expectedEvents - 1),
				tr.cnt,
			)
		}
	}
}
