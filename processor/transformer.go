package processor

import (
	"io"
	"log"
	"time"

	"github.com/TwitchScience/spade/parser"
	"github.com/TwitchScience/spade/transformer"
	"github.com/TwitchScience/spade/writer"
)

type RequestTransformer struct {
	t      transformer.Transformer
	T      *GobTransport
	closer chan bool
}

func (p *RequestTransformer) Close() {
	p.closer <- true
}

func (p *RequestTransformer) Process(e *parser.MixpanelEvent) (request *writer.WriteRequest) {
	defer func() {
		if recovered := recover(); recovered != nil {
			request = writer.MakeErrorRequest(e, recovered)
		}
	}()

	return p.t.Consume(e)
}

func (p *RequestTransformer) Listen(w writer.SpadeWriter) {
	ticker := time.Tick(10 * time.Microsecond)
	for {
		select {
		case <-p.closer:
			return
		case <-ticker:
			// can we reuse?
			var event parser.MixpanelEvent
			err := p.T.Read(&event)
			if err != nil && err != io.EOF {
				event = *parser.MakeBadEncodedEvent()
				log.Printf("got a reader error: %v\n", err)
			} else if err == nil {
				w.Write(p.Process(&event))
			}
		}
	}
}
