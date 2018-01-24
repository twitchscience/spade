package writer

import (
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
)

const (
	inboundChannelBuffer = 20000
	maxNonTrackedLogSize = 1 << 29 // 500MB
)

var (
	// EventsDir is the local subdirectory where successfully-transformed events are written.
	EventsDir = "events"
	// NonTrackedDir is the local subdirectory where non-tracked events are written.
	NonTrackedDir = "nontracked"
)

// SpadeWriter is an interface for writing to external sinks, like S3 or Kinesis.
type SpadeWriter interface {
	Write(*WriteRequest)
	Close() error

	// Rotate requests a rotation from the SpadeWriter, which *may* write to S3 or Kinesis
	// depending on timing and amount of information already buffered.  This should be
	// called periodically, as this is the only time a SpadeWriter will write to its
	// sink (except on Close).  It returns a bool indicating whether all sinks were
	// written to and one of the errors which arose in writing (if any).
	Rotate() (bool, error)
}

type rotateResult struct {
	allDone bool // did the rotation close all the files
	err     error
}

type writerController struct {
	SpadeFolder       string
	Routes            map[string]SpadeWriter
	Reporter          reporter.Reporter
	blueprintUploader *uploader.UploaderPool
	// The writer for the untracked events.
	NonTrackedWriter SpadeWriter

	// WriteRequests are sent to this channel if they need a new writerManager.
	newWriterChan chan *WriteRequest
	closeChan     chan error
	rotateChan    chan chan rotateResult

	maxLogBytes             int64
	maxLogAgeSecs           int64
	nontrackedMaxLogAgeSecs int64
	writerFactory           writerFactory
	sync.RWMutex
}

// writerManager manages writing for a single event type, creating a writer on demand
// and closing the writer after rotation.
type writerManager struct {
	writer        SpadeWriter
	writerType    string
	writeChan     chan *WriteRequest
	rotateChan    chan chan rotateResult
	closeChan     chan error
	writerFactory writerFactory
}

func (w *writerManager) Write(req *WriteRequest) {
	w.writeChan <- req
}

func (w *writerManager) Close() error {
	close(w.writeChan)
	x := <-w.closeChan
	return x
}

func (w *writerManager) Rotate() (bool, error) {
	receive := make(chan rotateResult)
	defer close(receive)
	w.rotateChan <- receive
	result := <-receive
	return result.allDone, result.err
}

func (w *writerManager) rotate() rotateResult {
	rotated, err := w.writer.Rotate()
	if err != nil {
		return rotateResult{false, fmt.Errorf("rotating %s: %v", w.writerType, err)}
	}
	if !rotated {
		return rotateResult{false, nil}
	}
	return rotateResult{true, nil}
}

func (w *writerManager) Listen() {
	for {
		select {
		case send := <-w.rotateChan:
			var result rotateResult
			if w.writer != nil {
				result = w.rotate()
				if result.allDone {
					w.writer = nil
				}
			} else {
				result = rotateResult{false, nil}
			}
			send <- result
		case req, ok := <-w.writeChan:
			var err error
			if !ok {
				if w.writer != nil {
					err = w.writer.Close()
				}
				w.closeChan <- err
				return
			}
			if w.writer == nil {
				w.writer, err = w.writerFactory.newWriter(w.writerType)
				if err != nil {
					logger.WithError(err).WithField("writerType", w.writerType).Error(
						"Error creating writer")
					continue
				}
			}
			w.writer.Write(req)
		}
	}

}

func newWriterManager(
	wf writerFactory,
	writerType string,
) *writerManager {
	return &writerManager{
		writerType:    writerType,
		writeChan:     make(chan *WriteRequest, inboundChannelBuffer),
		rotateChan:    make(chan chan rotateResult),
		closeChan:     make(chan error),
		writerFactory: wf,
	}
}

type writerFactory interface {
	newWriter(writerType string) (SpadeWriter, error)
}

type gzipWriterFactory struct {
	bufferPath string
	reporter   reporter.Reporter
	uploader   *uploader.UploaderPool
	rotateOn   RotateConditions
}

func (g *gzipWriterFactory) newWriter(writerType string) (SpadeWriter, error) {
	return newGzipWriter(
		g.bufferPath,
		writerType,
		g.reporter,
		g.uploader,
		g.rotateOn,
	)
}

// NewWriterController returns a writerController that handles logic to distribute
// writes across a number of workers.
// Each worker owns and operates one file. There are several sets of workers.
// Each set corresponds to a event type. Thus if we are processing a log
// file with 2 types of events we should produce (nWriters * 2) files
func NewWriterController(
	folder string,
	reporter reporter.Reporter,
	spadeUploaderPool *uploader.UploaderPool,
	blueprintUploaderPool *uploader.UploaderPool,
	maxLogBytes int64,
	maxLogAgeSecs int64,
	nontrackedMaxLogAgeSecs int64,
) SpadeWriter {
	c := &writerController{
		SpadeFolder:       folder,
		Routes:            make(map[string]SpadeWriter),
		Reporter:          reporter,
		blueprintUploader: blueprintUploaderPool,

		newWriterChan: make(chan *WriteRequest, 200),
		closeChan:     make(chan error),
		rotateChan:    make(chan chan rotateResult),

		maxLogBytes:             maxLogBytes,
		maxLogAgeSecs:           maxLogAgeSecs,
		nontrackedMaxLogAgeSecs: nontrackedMaxLogAgeSecs,
	}
	c.initNonTrackedWriter()
	c.writerFactory = &gzipWriterFactory{
		path.Join(folder, EventsDir),
		reporter,
		spadeUploaderPool,
		RotateConditions{
			MaxLogSize:     maxLogBytes,
			MaxTimeAllowed: time.Duration(maxLogAgeSecs) * time.Second,
		},
	}

	logger.Go(c.Listen)
	return c
}

func (c *writerController) writerCreator() {
	for req := range c.newWriterChan {
		category := req.GetCategory()
		writer := c.createWriter(category)
		writer.Write(req)
	}
}

func (c *writerController) createWriter(category string) SpadeWriter {
	c.Lock()
	defer c.Unlock()
	writer, hasWriter := c.Routes[category]
	if hasWriter {
		return writer
	}
	newWriter := newWriterManager(
		c.writerFactory,
		category,
	)
	logger.Go(newWriter.Listen)
	c.Routes[category] = newWriter
	return newWriter
}

func (c *writerController) Write(req *WriteRequest) {
	switch req.Failure {
	// Success case
	case reporter.None, reporter.SkippedColumn:
		category := req.GetCategory()
		c.RLock()
		writer, hasWriter := c.Routes[category]
		c.RUnlock()
		if hasWriter {
			writer.Write(req)
		} else {
			c.newWriterChan <- req
		}

	// Log non tracking events for blueprint
	case reporter.NonTrackingEvent:
		c.NonTrackedWriter.Write(req)

	// Otherwise tell the reporter that we got the event but it failed somewhere.
	default:
		c.Reporter.Record(req.GetResult())
	}
}

func (c *writerController) Listen() {
	logger.Go(c.writerCreator)
	for send := range c.rotateChan {
		send <- c.rotate()
	}
	c.closeChan <- c.close()
}

func (c *writerController) initNonTrackedWriter() {
	writerFactory := &gzipWriterFactory{
		path.Join(c.SpadeFolder, NonTrackedDir),
		c.Reporter,
		c.blueprintUploader,
		RotateConditions{
			MaxLogSize:     maxNonTrackedLogSize,
			MaxTimeAllowed: time.Duration(c.nontrackedMaxLogAgeSecs) * time.Second,
		},
	}
	w := newWriterManager(
		writerFactory,
		"nontracked",
	)
	logger.Go(w.Listen)
	c.NonTrackedWriter = w
}

func (c *writerController) Close() error {
	close(c.rotateChan)
	return <-c.closeChan
}

func (c *writerController) close() error {
	for _, w := range c.Routes {
		err := w.Close()
		if err != nil {
			return err
		}
	}

	return c.NonTrackedWriter.Close()
}

func (c *writerController) Rotate() (bool, error) {
	receive := make(chan rotateResult)
	defer close(receive)
	c.rotateChan <- receive
	result := <-receive
	return result.allDone, result.err
}

func (c *writerController) rotate() rotateResult {
	c.RLock()
	defer c.RUnlock()
	allRotated := true
	for _, w := range c.Routes {
		rotated, err := w.Rotate()
		if err != nil {
			return rotateResult{false, err}
		}
		if !rotated {
			allRotated = false
		}
	}

	ntRotated, err := c.NonTrackedWriter.Rotate()
	if err != nil {
		return rotateResult{false, err}
	}

	return rotateResult{ntRotated && allRotated, err}
}

// MakeErrorRequest returns a WriteRequest indicating panic happened during processing.
func MakeErrorRequest(e *parser.MixpanelEvent, err interface{}) *WriteRequest {
	return &WriteRequest{
		Category: "Unknown",
		Line:     "",
		UUID:     "error",
		Source:   e.Properties,
		Failure:  reporter.PanickedInProcessing,
		Pstart:   e.Pstart,
	}
}
