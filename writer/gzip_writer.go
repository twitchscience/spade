package writer

import (
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/spade/gzip_pool"
	"github.com/twitchscience/spade/reporter"
	spade_uploader "github.com/twitchscience/spade/uploader"
)

var (
	gzPool = gzip_pool.New(32)
)

type RotateConditions struct {
	MaxLogSize     int64
	MaxTimeAllowed time.Duration
}

func NewGzipWriter(
	folder, subfolder, writerType string,
	reporter reporter.Reporter,
	uploader *uploader.UploaderPool,
	rotateOn RotateConditions,
) (SpadeWriter, error) {
	path := folder + "/" + subfolder
	dirErr := os.MkdirAll(path, 0766)
	if dirErr != nil {
		return nil, dirErr
	}
	filename := getFilename(path, writerType)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	gzWriter := gzPool.Get(file)
	writer := &gzipFileWriter{
		ParentFolder:     folder,
		FullName:         filename,
		File:             file,
		GzWriter:         gzWriter,
		Reporter:         reporter,
		uploader:         uploader,
		RotateConditions: rotateOn,

		in: make(chan *WriteRequest),
	}
	writer.Add(1)
	go writer.Listen()

	return writer, nil
}

type gzipFileWriter struct {
	sync.WaitGroup
	ParentFolder     string
	FullName         string
	File             *os.File
	GzWriter         *gzip.Writer
	Reporter         reporter.Reporter
	uploader         *uploader.UploaderPool
	RotateConditions RotateConditions

	in chan *WriteRequest
}

// Rotate the logs if necessary.
func (w *gzipFileWriter) Rotate() (bool, error) {
	inode, err := w.File.Stat()
	if err != nil {
		return false, err
	}

	if ok, _ := isRotateNeeded(inode, w.FullName, w.RotateConditions); ok {
		err = w.Close()
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (w *gzipFileWriter) Close() error {
	defer gzPool.Put(w.GzWriter)
	close(w.in)
	w.Wait()

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
	dirErr := os.MkdirAll(w.ParentFolder+"/upload/", 0766)
	if dirErr != nil {
		return dirErr
	}

	// We have to move the file so that we are free to
	// overwrite this file next log processed.
	rotatedFileName := fmt.Sprintf("%s/upload/%s.gz",
		w.ParentFolder, inode.Name())

	os.Rename(w.FullName, rotatedFileName)
	spade_uploader.SafeGzipUpload(w.uploader, rotatedFileName)
	return nil
}

func (w *gzipFileWriter) Write(req *WriteRequest) error {
	w.in <- req
	return nil
}

func (w *gzipFileWriter) Listen() {
	defer w.Done()
	for {
		req, ok := <-w.in
		if !ok {
			return
		}
		_, err := w.GzWriter.Write([]byte(req.Line + "\n"))
		if err != nil {

			log.Printf("Failed Write: %v\n", err)
			w.Reporter.Record(&reporter.Result{
				Failure:    reporter.FAILED_WRITE,
				UUID:       req.UUID,
				Line:       req.Line,
				Category:   req.Category,
				FinishedAt: time.Now(),
				Duration:   time.Now().Sub(req.Pstart),
			})
		} else {
			w.Reporter.Record(req.GetResult())
		}
	}
}
