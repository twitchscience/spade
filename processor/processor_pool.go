package processor

import (
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/writer"
)

const (
	nConverters   = 10
	nTransformers = 10
)

// QueueSize is the size of the buffer on the input and output channels for the pool.
const QueueSize = 400000

type Pool interface {
	StartListeners()
	Process(parser.Parseable)
	Close()
}

// SpadeProcessorPool is pool of RequestConverters and RequestTransformers.
type SpadeProcessorPool struct {
	in           chan parser.Parseable
	converters   []*RequestConverter
	transformers []*RequestTransformer
	writer       writer.SpadeWriter
}

// BuildProcessorPool builds a new SpadeProcessorPool.
func BuildProcessorPool(configs transformer.ConfigLoader, rep reporter.Reporter,
	writer writer.SpadeWriter) *SpadeProcessorPool {

	transformers := make([]*RequestTransformer, nTransformers)
	converters := make([]*RequestConverter, nConverters)

	requestChannel := make(chan parser.Parseable, QueueSize)
	transport := make(chan parser.MixpanelEvent, QueueSize)

	for i := 0; i < nConverters; i++ {
		converters[i] = &RequestConverter{
			r:      rep,
			parser: parser.BuildSpadeParser(),
			in:     requestChannel,
			out:    transport,
			closer: make(chan bool),
		}
	}

	for i := 0; i < nTransformers; i++ {
		transformers[i] = &RequestTransformer{
			t:      transformer.NewRedshiftTransformer(configs),
			in:     transport,
			closer: make(chan bool),
		}
	}

	return &SpadeProcessorPool{
		in:           requestChannel,
		converters:   converters,
		transformers: transformers,
		writer:       writer,
	}
}

// Close closes all converters and trnasformers in the pool.
// Important: Ensure pool is drained before calling close.
func (p *SpadeProcessorPool) Close() {
	for _, worker := range p.converters {
		worker.Close()
	}
	for _, worker := range p.transformers {
		worker.Close()
	}
}

// StartListeners starts up goroutines for the converters and transformers.
func (p *SpadeProcessorPool) StartListeners() {
	for _, worker := range p.transformers {
		w := worker
		logger.Go(func() {
			w.Listen(p.writer)
		})
	}
	for _, worker := range p.converters {
		w := worker
		logger.Go(w.Listen)
	}
}

// Process submits the given Parseable to the pool for converting/transforming.
func (p *SpadeProcessorPool) Process(request parser.Parseable) {
	p.in <- request
}
