package processor

import (
	"github.com/TwitchScience/spade/parser"
	"github.com/TwitchScience/spade/reporter"
	"github.com/TwitchScience/spade/transformer"
	"github.com/TwitchScience/spade/writer"
)

const queueSize = 400000

type SpadeProcessorPool struct {
	in           chan *parser.ParseRequest
	converters   []*RequestConverter
	transformers []*RequestTransformer
}

func BuildProcessorPool(nConverters, nTransformers int,
	configs transformer.ConfigLoader, rep reporter.Reporter) *SpadeProcessorPool {

	transformers := make([]*RequestTransformer, nTransformers)
	converters := make([]*RequestConverter, nConverters)

	requestChannel := make(chan *parser.ParseRequest, queueSize)
	transport := NewGobTransport(NewBufferedTransport())

	for i := 0; i < nConverters; i++ {
		converters[i] = &RequestConverter{
			r:      rep,
			parser: parser.BuildSpadeParser(rep),
			in:     requestChannel,
			T:      transport,
			closer: make(chan bool),
		}
	}

	for i := 0; i < nTransformers; i++ {
		transformers[i] = &RequestTransformer{
			t:      transformer.NewRedshiftTransformer(configs),
			T:      transport,
			closer: make(chan bool),
		}
	}

	return &SpadeProcessorPool{
		in:           requestChannel,
		converters:   converters,
		transformers: transformers,
	}
}

// Important: Ensure pool is drained before calling close.
func (p *SpadeProcessorPool) Close() {
	for _, worker := range p.converters {
		worker.Close()
	}
	for _, worker := range p.transformers {
		worker.Close()
	}
}

// Resets the Processing Pool to use the new writer
func (p *SpadeProcessorPool) Listen(w writer.SpadeWriter) {
	for _, worker := range p.transformers {
		go worker.Listen(w)
	}
	for _, worker := range p.converters {
		go worker.Listen()
	}
}

func (p *SpadeProcessorPool) Process(request *parser.ParseRequest) {
	p.in <- request
}
