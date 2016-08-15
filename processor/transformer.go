package processor

import (
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/writer"
)

// RequestTransformer transforms MixpanelEvents and writes them out to a SpadeWriter.
type RequestTransformer struct {
	t      transformer.Transformer
	in     <-chan parser.MixpanelEvent
	closer chan bool
}

// Close stops the transformer's Listen() method.
func (p *RequestTransformer) Close() {
	p.closer <- true
}

// Process transforms the given event into a WriteRequest.
func (p *RequestTransformer) Process(e *parser.MixpanelEvent) (request *writer.WriteRequest) {
	defer func() {
		if recovered := recover(); recovered != nil {
			request = writer.MakeErrorRequest(e, recovered)
		}
	}()

	return p.t.Consume(e)
}

// Listen listens for incoming events, transforms them, and writes them to the SpadeWriter.
func (p *RequestTransformer) Listen(w writer.SpadeWriter) {
	for {
		select {
		case <-p.closer:
			return
		case event := <-p.in:
			w.Write(p.Process(&event))
		}
	}
}
