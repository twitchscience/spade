package processor

import (
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
)

type RequestConverter struct {
	r      reporter.Reporter
	in     chan parser.Parseable
	out    chan<- parser.MixpanelEvent
	closer chan bool
	parser parser.Parser
}

func (p *RequestConverter) Process(r parser.Parseable) (events []parser.MixpanelEvent, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			events = []parser.MixpanelEvent{
				*parser.MakePanicedEvent(r),
			}
		}
	}()
	return p.parser.Parse(r)
}

func (p *RequestConverter) Close() {
	p.closer <- true
}

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
