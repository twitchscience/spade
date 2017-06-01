package writer

import (
	"compress/gzip"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/spade/gzpool"
	"github.com/twitchscience/spade/reporter"
)

var (
	gzPool = gzpool.New(32)
)

// RotateConditions is the parameters for maximum time/size until we force a rotation.
type RotateConditions struct {
	MaxLogSize     int64
	MaxTimeAllowed time.Duration
}

// NewGzipWriter returns a gzipFileWriter, a pool of gzip goroutines that report results.
func NewGzipWriter(
	folder, subfolder, writerType string,
	reporter reporter.Reporter,
	uploader *uploader.UploaderPool,
	rotateOn RotateConditions,
) (SpadeWriter, error) {
	path := folder + "/" + subfolder
	// Append a period to keep the version separate from the TempFile suffix.
	file, err := ioutil.TempFile(path, writerType+".")
	if err != nil {
		return nil, err
	}

	writer := &gzipFileWriter{
		File:             file,
		GzWriter:         gzPool.Get(file),
		Reporter:         reporter,
		uploader:         uploader,
		RotateConditions: rotateOn,

		in: make(chan *WriteRequest),
	}
	writer.Add(1)
	logger.Go(writer.Listen)

	return writer, nil
}

type gzipFileWriter struct {
	sync.WaitGroup
	File             *os.File
	GzWriter         *gzip.Writer
	Reporter         reporter.Reporter
	uploader         *uploader.UploaderPool
	RotateConditions RotateConditions

	in chan *WriteRequest
}

// Rotate rotates the logs if necessary. This must be called at a regular interval.
func (w *gzipFileWriter) Rotate() (bool, error) {
	inode, err := w.File.Stat()
	if err != nil {
		return false, err
	}

	if ok, _ := isRotateNeeded(inode, w.RotateConditions); ok {
		err = w.Close()
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

// Close closes the input channel, flushes all inputs, then flushes all state.
func (w *gzipFileWriter) Close() error {
	defer gzPool.Put(w.GzWriter)
	close(w.in)
	w.Wait()

	if gzCloseErr := w.GzWriter.Close(); gzCloseErr != nil {
		return gzCloseErr
	}

	if closeErr := w.File.Close(); closeErr != nil {
		return closeErr
	}

	w.uploader.Upload(&uploader.UploadRequest{
		Filename: w.File.Name(),
		FileType: uploader.Gzip,
	})
	return nil
}

// Write submits a line to be written by the pool.
func (w *gzipFileWriter) Write(req *WriteRequest) {
	w.in <- req
}

// Listen is a blocking method that processes input and reports the result of writing it.
func (w *gzipFileWriter) Listen() {
	defer w.Done()
	for {
		req, ok := <-w.in
		if !ok {
			return
		}
		_, err := w.GzWriter.Write([]byte(req.Line + "\n"))
		if err != nil {
			logger.WithError(err).Error("Failed to write to gzip")
			w.Reporter.Record(&reporter.Result{
				Failure:    reporter.FailedWrite,
				UUID:       req.UUID,
				Line:       req.Line,
				Category:   req.Category,
				FinishedAt: time.Now(),
				Duration:   time.Since(req.Pstart),
			})
		} else {
			w.Reporter.Record(req.GetResult())
		}
	}
}
