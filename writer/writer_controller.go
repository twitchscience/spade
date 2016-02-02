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
	maxLogSize        = getInt64FromEnv("MAX_LOG_BYTES", 1<<32)                                  // default 4GB
	maxLogTimeAllowed = time.Duration(getInt64FromEnv("MAX_LOG_AGE_SECS", 300*60)) * time.Second // default 5 hours

	maxNonTrackedLogSize        = getInt64FromEnv("MAX_UNTRACKED_LOG_BYTES", 1<<29)                                 // default 500MB
	maxNonTrackedLogTimeAllowed = time.Duration(getInt64FromEnv("MAX_UNTRACKED_LOG_AGE_SECS", 10*60)) * time.Second // default 10 mins

	EventsDir     = "events"
	NonTrackedDir = "nontracked"
)

func getInt64FromEnv(target string, def int64) int64 {
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
	Rotate() (bool, error)
}

type writerController struct {
	SpadeFolder       string
	Routes            map[string]SpadeWriter
	Reporter          reporter.Reporter
	redshiftUploader  *uploader.UploaderPool
	blueprintUploader *uploader.UploaderPool
	// The writer for the untracked events.
	NonTrackedWriter SpadeWriter

	inbound    chan *WriteRequest
	closeChan  chan chan error
	rotateChan chan chan error
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
	c := &writerController{
		SpadeFolder:       folder,
		Routes:            make(map[string]SpadeWriter),
		Reporter:          reporter,
		redshiftUploader:  spadeUploaderPool,
		blueprintUploader: blueprintUploaderPool,

		inbound:    make(chan *WriteRequest, inboundChannelBuffer),
		closeChan:  make(chan chan error),
		rotateChan: make(chan chan error),
	}
	err := c.initNonTrackedWriter()
	if err != nil {
		return nil, err
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
		case send := <-c.rotateChan:
			send <- c.rotate()
		case req := <-c.inbound:
			c.route(req)
		}
	}
}

func (c *writerController) route(request *WriteRequest) error {
	switch request.Failure {
	// Success case
	case reporter.NONE, reporter.SKIPPED_COLUMN:
		category := request.GetCategory()
		if _, hasWriter := c.Routes[category]; !hasWriter {
			newWriter, err := NewGzipWriter(
				c.SpadeFolder,
				EventsDir,
				category,
				c.Reporter,
				c.redshiftUploader,
				RotateConditions{
					MaxLogSize:     maxLogSize,
					MaxTimeAllowed: maxLogTimeAllowed,
				},
			)
			if err != nil {
				return err
			}
			c.Routes[category] = newWriter
		}
		c.Routes[category].Write(request)

	// Log non tracking events for blueprint
	case reporter.NON_TRACKING_EVENT:
		c.NonTrackedWriter.Write(request)

	// Otherwise tell the reporter that we got the event but it failed somewhere.
	default:
		c.Reporter.Record(request.GetResult())
	}
	return nil
}

func (c *writerController) initNonTrackedWriter() error {
	w, err := NewGzipWriter(
		c.SpadeFolder,
		NonTrackedDir,
		"nontracked",
		c.Reporter,
		c.blueprintUploader,
		RotateConditions{
			MaxLogSize:     maxNonTrackedLogSize,
			MaxTimeAllowed: maxNonTrackedLogTimeAllowed,
		},
	)
	if err != nil {
		return err
	}
	c.NonTrackedWriter = w
	return nil
}

func (c *writerController) Close() error {
	recieve := make(chan error)
	defer close(recieve)
	c.closeChan <- recieve
	return <-recieve
}

func (c *writerController) close() error {
	for _, w := range c.Routes {
		err := w.Close()
		if err != nil {
			return err
		}
	}

	err := c.NonTrackedWriter.Close()
	if err != nil {
		return err
	}

	return c.initNonTrackedWriter()
}

func (c *writerController) Rotate() (bool, error) {
	recieve := make(chan error)
	defer close(recieve)
	c.rotateChan <- recieve
	return len(c.Routes) == 0, <-recieve
}

func (c *writerController) rotate() error {
	for k, w := range c.Routes {
		rotated, err := w.Rotate()
		if err != nil {
			return err
		}
		if rotated {
			delete(c.Routes, k)
		}
	}

	rotated, err := c.NonTrackedWriter.Rotate()
	if err != nil {
		return err
	}

	if rotated {
		c.initNonTrackedWriter()
	}

	return nil
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
