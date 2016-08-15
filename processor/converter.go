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
	closer chan bool
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

// Close closes the RequestConverter by stopping the Listen() method.
func (p *RequestConverter) Close() {
	p.closer <- true
}

// Listen sits in a loop Processing requests until the RequestConverter is Closed.
func (p *RequestConverter) Listen() {
	for {
		select {
		case request := <-p.in:
			// ignore the error here beause the event will have status to report it.
			events, _ := p.Process(request)
			for _, event := range events {
				p.out <- event
			}
		case <-p.closer:
			return
		}
	}
}
