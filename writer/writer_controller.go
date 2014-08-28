package writer

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
)

const (
	inboundChannelBuffer = 400000
)

var (
	MaxLogSize     = loadFromEnv("MAX_LOG_SIZE", 1<<32)                                     // default 4GB
	MaxTimeAllowed = time.Duration(loadFromEnv("MAX_LOG_LIFETIME_MINS", 300)) * time.Minute // default 5 hours

	EventsDir     = "events"
	NonTrackedDir = "nontracked"
)

func loadFromEnv(target string, def int64) int64 {
	env := os.Getenv(target)
	if env == "" {
		return def
	}
	i, err := strconv.ParseInt(env, 10, 64)
	if err != nil {
		return def
	}
	return i
}

type SpadeWriter interface {
	Write(*WriteRequest) error
	Close() error
	Reset() error
}

type writerController struct {
	SpadeFolder       string
	Routes            map[string]SpadeWriter
	Reporter          reporter.Reporter
	redshiftUploader  *uploader.UploaderPool
	blueprintUploader *uploader.UploaderPool
	// The writer for the untracked events.
	NonTrackedWriter SpadeWriter

	inbound   chan *WriteRequest
	closeChan chan chan error
	resetChan chan chan error
}

// The WriterController handles logic to distribute writes across a number of workers.
// Each worker owns and operates one file. There are several sets of workers.
// Each set corresponds to a event type. Thus if we are processing a log
// file with 2 types of events we should produce (nWriters * 2) files
func NewWriterController(
	folder string,
	reporter reporter.Reporter,
	spadeUploaderPool *uploader.UploaderPool,
	blueprintUploaderPool *uploader.UploaderPool,
) (SpadeWriter, error) {
	w, err := NewGzipWriter(
		folder,
		NonTrackedDir,
		"nontracked",
		reporter,
		blueprintUploaderPool,
	)
	if err != nil {
		return nil, err
	}
	c := &writerController{
		SpadeFolder:       folder,
		Routes:            make(map[string]SpadeWriter),
		Reporter:          reporter,
		redshiftUploader:  spadeUploaderPool,
		blueprintUploader: blueprintUploaderPool,
		NonTrackedWriter:  w,

		inbound:   make(chan *WriteRequest, inboundChannelBuffer),
		closeChan: make(chan chan error),
		resetChan: make(chan chan error),
	}
	go c.Listen()
	return c, nil
}

// we put the event name in twice so that everything has a
// common prefix when we upload to s3
func getFilename(path, writerCategory string) string {
	return fmt.Sprintf("%s/%s.gz", path, writerCategory)
}

func (c *writerController) Write(req *WriteRequest) error {
	c.inbound <- req
	return nil
}

// TODO better Error handling
func (c *writerController) Listen() {
	for {
		select {
		case send := <-c.closeChan:
			send <- c.close()
		case send := <-c.resetChan:
			err := c.close()
			c.reset()
			send <- err
		case req := <-c.inbound:
			c.route(req)
		}
	}
}

func (controller *writerController) route(request *WriteRequest) error {
	switch request.Failure {
	// Success case
	case reporter.NONE, reporter.SKIPPED_COLUMN:
		category := request.GetCategory()
		if _, hasWriter := controller.Routes[category]; !hasWriter {
			newWriter, err := NewGzipWriter(
				controller.SpadeFolder,
				EventsDir,
				category,
				controller.Reporter,
				controller.redshiftUploader,
			)
			if err != nil {
				return err
			}
			controller.Routes[category] = newWriter
		}
		controller.Routes[category].Write(request)

	// Log non tracking events for blueprint
	case reporter.NON_TRACKING_EVENT:
		controller.NonTrackedWriter.Write(request)

	// Otherwise tell the reporter that we got the event but it failed somewhere.
	default:
		controller.Reporter.Record(request.GetResult())
	}
	return nil
}

func (c *writerController) Close() error {
	recieve := make(chan error)
	defer close(recieve)
	c.closeChan <- recieve
	return <-recieve
}

func (controller *writerController) close() error {
	for _, w := range controller.Routes {
		err := w.Close()
		if err != nil {
			return err
		}
	}

	err := controller.NonTrackedWriter.Close()
	if err != nil {
		return err
	}

	w, err := NewGzipWriter(
		controller.SpadeFolder,
		NonTrackedDir,
		"nontracked",
		controller.Reporter,
		controller.blueprintUploader,
	)
	if err != nil {
		return err
	}
	controller.NonTrackedWriter = w
	return nil
}

func (c *writerController) Reset() error {
	recieve := make(chan error)
	defer close(recieve)
	c.resetChan <- recieve
	return <-recieve
}

func (c *writerController) reset() {
	for k, _ := range c.Routes {
		delete(c.Routes, k)
	}
}

func MakeErrorRequest(e *parser.MixpanelEvent, err interface{}) *WriteRequest {
	return &WriteRequest{
		Category: "Unknown",
		Line:     "",
		UUID:     "error",
		Source:   e.Properties,
		Failure:  reporter.PANICED_IN_PROCESSING,
		Pstart:   e.Pstart,
	}
}
