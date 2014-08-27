package writer

import (
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/spade/gzip_pool"
	"github.com/twitchscience/spade/reporter"
)

var (
	gzPool   = gzip_pool.New(16)
	EventDir = "events"
)

func NewGzipWriter(
	folder, writerType string,
	reporter reporter.Reporter,
	uploader *uploader.UploaderPool,
) (SpadeWriter, error) {
	path := folder + "/" + EventDir
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
		ParentFolder: folder,
		FullName:     filename,
		File:         file,
		GzWriter:     gzWriter,
		in:           make(chan *WriteRequest),
		Reporter:     reporter,
		uploader:     uploader,
	}
	go writer.Listen()

	return writer, nil
}

type gzipFileWriter struct {
	ParentFolder string
	FullName     string
	File         *os.File
	GzWriter     *gzip.Writer
	Reporter     reporter.Reporter
	uploader     *uploader.UploaderPool

	in chan *WriteRequest
}

func (w *gzipFileWriter) Close() error {
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

func (w *gzipFileWriter) Reset() error {
	return nil
}

func (w *gzipFileWriter) Write(req *WriteRequest) error {
	w.in <- req
	return nil
}

func (w *gzipFileWriter) Listen() {
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
				FinishedAt: time.Now(),
				Duration:   time.Now().Sub(req.Pstart),
			})
		} else {
			w.Reporter.Record(req.GetResult())
		}
	}
}
