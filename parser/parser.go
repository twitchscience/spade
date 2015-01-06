package parser

import (
	"fmt"
	"time"

	"github.com/twitchscience/spade/reporter"
)

type parserEntry struct {
	name string
	p    Parser
}

var parsers = []parserEntry{}

// Register makes Parsers available to parse lines. Each Parser should
// provide a mechanism to register themselves with this Registry. The
// order in which parsers are registered are the order in which
// they'll be called when attempting to parse a Parseable
func Register(name string, p Parser) error {
	if p == nil {
		return fmt.Errorf("parser: Register parser is nil")
	}
	for _, k := range parsers {
		if k.name == name {
			return fmt.Errorf("parser: Register called twice for parser: %s", name)
		}
	}
	parsers = append(parsers, parserEntry{
		name: name,
		p:    p,
	})
	return nil
}

func clearAll() {
	parsers = []parserEntry{}
}

type fanoutParser struct {
	reporter reporter.Reporter
}

func (f *fanoutParser) Parse(line Parseable) (events []MixpanelEvent, err error) {
	numParsers := len(parsers)
	if numParsers == 0 {
		return nil, fmt.Errorf("parser: no parsers registered")
	}

	for _, entry := range parsers {
		if mes, e := entry.p.Parse(line); e != nil && events == nil {
			events = mes
			err = e
		} else {
			events = mes
			err = e
			if len(events) > 1 {
				f.reporter.IncrementExpected(len(events) - 1)
			}
			// return the first successful parse
			break
		}
	}
	return
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
