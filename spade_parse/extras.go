package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/parser/nginx"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/table_config"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade_edge/request_handler"

	"github.com/twitchscience/spade/writer"
)

type dummyReporter struct{}

func (d *dummyReporter) Record(c *reporter.Result) {}
func (d *dummyReporter) IncrementExpected(n int)   {}
func (d *dummyReporter) Reset()                    {}
func (d *dummyReporter) Finalize() map[string]int {
	return make(map[string]int)
}

type EZSpadeEdgeLogger struct {
	p *RequestParser
	w writer.SpadeWriter
}

type StdoutSpadeWriter struct {
	config map[string][]transformer.RedshiftType
}

type RequestParser struct {
	t      transformer.Transformer
	parser parser.Parser
}

type parseRequest struct {
	data  []byte
	start time.Time
}

func (p *parseRequest) Data() []byte {
	return p.data
}

func (p *parseRequest) StartTime() time.Time {
	return p.start
}

// This is for simple applications of the parser.
func BuildProcessor(configs map[string][]transformer.RedshiftType) *RequestParser {
	return &RequestParser{
		t: transformer.NewRedshiftTransformer(
			table_config.NewStaticLoader(configs),
		),
		parser: nginx.BuildSpadeParser(&dummyReporter{}),
	}
}

func (p *RequestParser) Process(request parser.Parseable) (result []writer.WriteRequest) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = []writer.WriteRequest{
				writer.WriteRequest{
					Category: "Unknown",
					Line:     fmt.Sprintf("%v", recovered),
					UUID:     "error",
					Source:   json.RawMessage(request.Data()),
					Failure:  reporter.PANICED_IN_PROCESSING,
					Pstart:   request.StartTime(),
				},
			}
		}
	}()
	// Parse out of base64'd json.
	events, _ := p.parser.Parse(request)

	// Transform to SpadeEvent.
	for _, e := range events {
		result = append(result, *p.t.Consume(&e))
	}
	return result
}

func (s *StdoutSpadeWriter) Write(req *writer.WriteRequest) error {
	fmt.Println("Spade Recorded your event as ", req.Category)
	fmt.Printf("It took %s to process your event\n", time.Now().Sub(req.Pstart))
	if req.Failure != reporter.NONE && req.Failure != reporter.SKIPPED_COLUMN {
		fmt.Println("\nSpade had the following errors when processing your event: ")
		fmt.Println(req.Failure.String())
		return nil
	}
	fmt.Println("\nYour event Transformed into redshift ingest format")
	fmt.Printf("%s\n", strings.Replace(req.Line, "\t", "\x1b[31m|\x1b[0m", -1))
	fmt.Println()
	return nil
}

func (s *StdoutSpadeWriter) Reset() error {
	return nil
}

func (s *StdoutSpadeWriter) Close() error {
	return nil
}

func (e *EZSpadeEdgeLogger) Log(r request_handler.EventRecord) {
	for _, request := range e.p.Process(e.edgeEventToParseRequest(r)) {
		e.w.Write(&request)
	}
}

func (e *EZSpadeEdgeLogger) Close() {}

func (e *EZSpadeEdgeLogger) edgeEventToParseRequest(r request_handler.EventRecord) parser.Parseable {
	return &parseRequest{
		data:  []byte(fmt.Sprintf("%v\n", r.HttpRequest())),
		start: time.Now(),
	}
}
