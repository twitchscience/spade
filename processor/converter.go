package processor

import (
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
)

// RequestConverter parses Parseables that come in into MixpanelEvents.
type RequestConverter struct {
	r      reporter.Reporter
	in     chan parser.Parseable
	out    chan<- parser.MixpanelEvent
	done   chan bool
	parser parser.Parser
}

// Process parses the given Parseable into a list of events.
func (p *RequestConverter) Process(r parser.Parseable) (events []parser.MixpanelEvent, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			events = []parser.MixpanelEvent{
				*parser.MakePanickedEvent(r),
			}
		}
	}()
	return p.parser.Parse(r)
}

// Wait waits for the input channel to flush.
func (p *RequestConverter) Wait() {
	<-p.done
}

// Listen sits in a loop Processing requests until the RequestConverter is Closed.
func (p *RequestConverter) Listen() {
	for request := range p.in {
		/* #nosec */ // any errors will be contained in the relevant events
		events, _ := p.Process(request)
		for _, event := range events {
			p.out <- event
		}
	}
	p.done <- true
}
