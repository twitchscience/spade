package writer

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/TwitchScience/aws_utils/uploader"
	"github.com/TwitchScience/spade/gzip_pool"
	"github.com/TwitchScience/spade/parser"
	"github.com/TwitchScience/spade/reporter"
)

const (
	EventDir             = "events"
	inboundChannelBuffer = 400000
)

var (
	MaxLogSize     = loadFromEnv("MAX_LOG_SIZE", 1<<32)                                     // default 4GB
	MaxTimeAllowed = time.Duration(loadFromEnv("MAX_LOG_LIFETIME_MINS", 300)) * time.Minute // default 5 hours
	gzPool         = gzip_pool.New(16)
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

type writerSet struct {
	ParentFolder string
	FullName     string
	File         *os.File
	GzWriter     *gzip.Writer
	in           chan *WriteRequest
	Reporter     reporter.Reporter
	uploader     *uploader.UploaderPool
}

type writerController struct {
	SpadeFolder string
	Routes      map[string]SpadeWriter
	Reporter    reporter.Reporter
	uploader    *uploader.UploaderPool

	inbound   chan *WriteRequest
	closeChan chan chan error
	resetChan chan chan error
}

type WriteRequest struct {
	Category string
	Line     string
	UUID     string
	// Keep the source around for logging
	Source  json.RawMessage
	Failure reporter.FailMode
	Pstart  time.Time
}

func (r *WriteRequest) GetStartTime() time.Time {
	return r.Pstart
}

func (r *WriteRequest) GetCategory() string {
	return r.Category
}

func (r *WriteRequest) GetMessage() string {
	return string(r.Source)
}

func (r *WriteRequest) GetResult() *reporter.Result {
	return &reporter.Result{
		Failure:    r.Failure,
		UUID:       r.UUID,
		Line:       r.Line,
		FinishedAt: time.Now(),
		Duration:   time.Now().Sub(r.Pstart),
	}
}

// The WriterController handles logic to distribute writes across a number of workers.
// Each worker owns and operates one file. There are several sets of workers.
// Each set corresponds to a event type. Thus if we are processing a log
// file with 2 types of events we should produce (nWriters * 2) files
func NewWriterController(reporter reporter.Reporter, folder string, spadeUploaderPool *uploader.UploaderPool) SpadeWriter {
	c := &writerController{
		SpadeFolder: folder,
		Routes:      make(map[string]SpadeWriter),
		inbound:     make(chan *WriteRequest, inboundChannelBuffer),
		closeChan:   make(chan chan error),
		resetChan:   make(chan chan error),
		Reporter:    reporter,
		uploader:    spadeUploaderPool,
	}
	go c.Listen()
	return c
}

// we put the event name in twice so that everything has a
// common prefix when we upload to s3
func getFilename(path string, r *WriteRequest) string {
	return fmt.Sprintf("%s/%s.gz", path, r.GetCategory())
}

func (c *writerController) getPath(r *WriteRequest) string {
	return c.SpadeFolder + "/" + EventDir
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
	if request.Failure != reporter.NONE && request.Failure != reporter.SKIPPED_COLUMN {
		controller.Reporter.Record(request.GetResult())
		return nil
	}
	category := request.GetCategory()
	if _, hasWriter := controller.Routes[category]; !hasWriter {
		path := controller.getPath(request)
		dirErr := os.MkdirAll(path, 0766)
		if dirErr != nil {
			return dirErr
		}
		filename := getFilename(path, request)
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return err
		}

		gzWriter := gzPool.Get(file)
		writer := &writerSet{
			ParentFolder: controller.SpadeFolder,
			FullName:     filename,
			File:         file,
			GzWriter:     gzWriter,
			in:           make(chan *WriteRequest),
			Reporter:     controller.Reporter,
			uploader:     controller.uploader,
		}
		go writer.Listen()

		controller.Routes[category] = writer
	}
	controller.Routes[category].Write(request)
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

func (w *writerSet) Close() error {
	defer gzPool.Put(w.GzWriter)
	close(w.in)

	inode, err := w.File.Stat()
	if err != nil {
		return err
	}
	if gzFlushErr := w.GzWriter.Flush(); gzFlushErr != nil {
		return gzFlushErr
	}
	if gzCloseErr := w.GzWriter.Close(); gzCloseErr != nil {
		return gzCloseErr
	}

	if closeErr := w.File.Close(); closeErr != nil {
		return closeErr
	}
	// Rotate the logs if necessary.
	if ok, _ := isRotateNeeded(inode, w.FullName); ok {
		dirErr := os.MkdirAll(w.ParentFolder+"/upload/", 0766)
		if dirErr != nil {
			return dirErr
		}
		// We have to move the file so that we are free to
		// overwrite this file next log processed.
		rotatedFileName := fmt.Sprintf("%s/upload/%s.gz",
			w.ParentFolder, inode.Name())
		os.Rename(w.FullName, rotatedFileName)
		w.uploader.Upload(&uploader.UploadRequest{
			Filename: rotatedFileName,
			FileType: uploader.Gzip,
		})
	}
	return nil
}

func (w *writerSet) Reset() error {
	return nil
}

func (writer *writerSet) Write(req *WriteRequest) error {
	writer.in <- req
	return nil
}

func (writer *writerSet) Listen() {
	for {
		req, ok := <-writer.in
		if !ok {
			return
		}
		_, err := writer.GzWriter.Write([]byte(req.Line + "\n"))
		if err != nil {

			log.Printf("Failed Write: %v\n", err)
			writer.Reporter.Record(&reporter.Result{
				Failure:    reporter.FAILED_WRITE,
				UUID:       req.UUID,
				Line:       req.Line,
				FinishedAt: time.Now(),
				Duration:   time.Now().Sub(req.Pstart),
			})
		} else {
			writer.Reporter.Record(req.GetResult())
		}
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
