package log_manager

import (
	"fmt"
	"log"
	"reflect"

	"github.com/TwitchScience/spade/processor"
	"github.com/TwitchScience/spade/reader"
	"github.com/TwitchScience/spade/reporter"
	"github.com/TwitchScience/spade/writer"
)

type LogParser struct {
	Reporter  reporter.Reporter
	Processor *processor.SpadeProcessorPool
	Writer    writer.SpadeWriter
	useGzip   bool
}

func (p *LogParser) parse(reader reader.LogReader) (map[string]int, error) {
	defer func() {
		p.Reporter.Reset()
		ferr := p.Writer.Reset()
		if ferr != nil {
			log.Println(ferr)
		}
	}()

	var err error
	for {
		request, err := reader.ProvideLine()
		if err == nil {
			p.Reporter.IncrementExpected(1)
			p.Processor.Process(request)
		} else {
			break
		}
	}

	stats := p.Reporter.Finalize()
	return stats, err
}

func (p *LogParser) process(task *LogTask) error {
	filename := task.Filename()
	logReader, err := reader.GetFileLogReader(filename, p.useGzip)
	defer func() {
		if logReader != nil {
			logReader.Close()
		}
	}()
	if err != nil {
		return fmt.Errorf("Got error while opening %v: %v", filename, err)
	}
	stats, parseErr := p.parse(logReader)
	log.Printf("Parsed %v - stats: %v\n", filename, stats)
	if parseErr != nil {
		log.Printf("Got error of while reading %v: %v", reflect.TypeOf(parseErr), filename, parseErr)
	}
	return nil
}
