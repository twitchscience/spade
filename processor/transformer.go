package processor

import (
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/writer"
)

type RequestTransformer struct {
	t      transformer.Transformer
	in     <-chan parser.MixpanelEvent
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
	for {
		select {
		case <-p.closer:
			return
		case event := <-p.in:
			w.Write(p.Process(&event))
		}
	}
}
