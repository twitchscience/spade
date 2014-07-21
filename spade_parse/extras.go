package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/TwitchScience/spade/parser"
	"github.com/TwitchScience/spade/reporter"
	"github.com/TwitchScience/spade/table_config"
	"github.com/TwitchScience/spade/transformer"

	"github.com/TwitchScience/spade/writer"
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

// This is for simple applications of the parser.
func BuildProcessor(configs map[string][]transformer.RedshiftType) *RequestParser {
	return &RequestParser{
		t: transformer.NewRedshiftTransformer(
			table_config.NewStaticLoader(configs),
		),
		parser: parser.BuildSpadeParser(&dummyReporter{}),
	}
}

func (p *RequestParser) Process(request *parser.ParseRequest) (result []writer.WriteRequest) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = []writer.WriteRequest{
				writer.WriteRequest{
					Category: "Unknown",
					Line:     fmt.Sprintf("%v", recovered),
					UUID:     "error",
					Source:   json.RawMessage(request.Target),
					Failure:  reporter.PANICED_IN_PROCESSING,
					Pstart:   request.Pstart,
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
	if req.Failure == reporter.NONE || req.Failure == reporter.SKIPPED_COLUMN {
		fmt.Fprintln(os.Stdout, req.Category)
	}
	// fmt.Println("Spade Recorded your event as ", req.Category)
	// fmt.Printf("It took %s to process your event\n", time.Now().Sub(req.Pstart))
	// if req.Failure != reporter.NONE {
	//  fmt.Println("\nSpade had the following errors when processing your event: ")
	//  fmt.Println(req.Failure.String())
	// }
	// fmt.Println("\nYour event Transformed into redshift ingest format")
	// fmt.Printf("%s\n", strings.Replace(req.Line, "\t", "\x1b[31m|\x1b[0m", -1))
	// fmt.Println()
	return nil
}

func (s *StdoutSpadeWriter) Reset() error {
	return nil
}

func (s *StdoutSpadeWriter) Close() error {
	return nil
}

func (e *EZSpadeEdgeLogger) Log(ip, data string, t time.Time, id string) error {
	for _, request := range e.p.Process(e.edgeEventToParseRequest(ip, data, t, id)) {
		e.w.Write(&request)
	}
	return nil
}

func (e *EZSpadeEdgeLogger) Close() {}

func (e *EZSpadeEdgeLogger) edgeEventToParseRequest(ip, data string, t time.Time, id string) *parser.ParseRequest {
	return &parser.ParseRequest{
		Target: []byte(fmt.Sprintf("%s [%d.000] data=%s %s", ip, t.Unix(), data, id)),
		Pstart: time.Now(),
	}
}
