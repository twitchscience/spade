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

func checkForParser(name string) bool {
	for _, p := range parsers {
		if p.name == name {
			return true
		}
	}
	return false
}

func TestRegisterAndClearing(t *testing.T) {
	parserName := "test_parser"
	Register(parserName, &singleEventParser{})
	if !checkForParser(parserName) {
		t.Fatalf("register: %s not present, expected it to be. Current parsers: %v", parserName, parsers)
	}
	if err := Register(parserName, &singleEventParser{}); err == nil {
		t.Fatal("register: expected error when registering duplicate named parser, didn't get one")
	}
	clearAll()
	if checkForParser(parserName) {
		t.Fatalf("register: %s present, expected it not to be. Current parsers: %v", parserName, parsers)
	}
	if err := Register("", nil); err == nil {
		t.Fatal("register: expected error when setting nil parser, didn't get one")
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
		t.Fatalf("parser: expected events to be nil, got %v", mes)
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

func setupParsers(ps ...Parser) Parser {
	clearAll()
	fop := BuildSpadeParser(&testReporter{})
	for i, p := range ps {
		Register(fmt.Sprintf("parser%d", i), p)
	}
	return fop
}

func TestMultiParserCall(t *testing.T) {
	p := setupParsers(&errorParser{}, &singleEventParser{})
	if _, err := p.Parse(&logLine{}); err != nil {
		t.Fatalf("multi parser: unexpected error %v", err)
	}

	p = setupParsers(&singleEventParser{}, &errorParser{})
	if _, err := p.Parse(&logLine{}); err != nil {
		t.Fatalf("multi parser: unexpected error %v", err)
	}

	p = setupParsers(&multiEventParser{}, &singleEventParser{}, &errorParser{})
	mes, err := p.Parse(&logLine{})
	if err != nil {
		t.Fatalf("multi parser: unexpected error %v", err)
	}
	if len(mes) != 2 {
		t.Fatalf("multi parser: incorrect event count returned. Expected %d, got %d", 2, len(mes))
	}
}
