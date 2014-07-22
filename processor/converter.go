package processor

import (
	"log"
	"time"

	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
)

func MakeTransportError(event *parser.MixpanelEvent) *reporter.Result {
	return &reporter.Result{
		Line:       "",
		Duration:   time.Now().Sub(event.Pstart),
		FinishedAt: time.Now(),
		UUID:       event.UUID,
		Failure:    reporter.FAILED_TRANSPORT,
	}
}

type RequestConverter struct {
	r      reporter.Reporter
	in     chan *parser.ParseRequest
	closer chan bool
	parser parser.Parser
	T      *GobTransport
}

func (p *RequestConverter) Process(r *parser.ParseRequest) (events []parser.MixpanelEvent, err error) {
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
				err := p.T.Write(event)
				if err != nil {
					log.Printf("Could not transport event: %v\n", err)
					p.r.Record(MakeTransportError(&event))
				}
			}
		case <-p.closer:
			return
		}
	}
}
