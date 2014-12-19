package parser

import (
	"fmt"
	"time"

	"github.com/twitchscience/spade/reporter"
)

var parsers = make(map[string]Parser)

// Register makes Parsers available to parse lines. Each Parser should
// provide a mechanism to register themselves with this Registry.
func Register(name string, p Parser) {
	if p == nil {
		panic("parser: Register parser is nil")
	}
	if _, dup := parsers[name]; dup {
		panic("parser: Register called twice for parser: " + name)
	}
	parsers[name] = p
}

func clearAll() {
	parsers = make(map[string]Parser)
}

type fanoutParser struct {
	reporter reporter.Reporter
}

func (f *fanoutParser) Parse(line Parseable) (events []MixpanelEvent, err error) {
	numParsers := len(parsers)
	if numParsers == 0 {
		return nil, fmt.Errorf("parser: no parsers registered")
	}

	for _, p := range parsers {
		events, err = p.Parse(line)
		if err != nil {
			return
		}
		if len(events) > 1 {
			f.reporter.IncrementExpected(len(events) - 1)
		}
		return
	}
	return nil, nil
}

func BuildSpadeParser(r reporter.Reporter) Parser {
	return &fanoutParser{
		reporter: r,
	}
}

type Parseable interface {
	Data() []byte
	StartTime() time.Time
}

type Parser interface {
	Parse(Parseable) ([]MixpanelEvent, error)
}

type URLEscaper interface {
	QueryUnescape([]byte) ([]byte, error)
}

type ParseResult interface {
	Data() []byte
	UUID() string
	Time() string
}
