package file_writer

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"

	"github.com/twitchscience/aws_utils/uploader"
)

type FileWriter struct {
	filename   string
	file       *os.File
	gzipWriter *gzip.Writer
	uploader   *uploader.UploaderPool
}

type FileWriterFactory struct {
	baseFilename string
	uploader     *uploader.UploaderPool
}

func BuildFileWriterFactory(filename string, uploader *uploader.UploaderPool) WriterFactory {
	return &FileWriterFactory{
		baseFilename: filename,
		uploader:     uploader,
	}
}

func (c *FileWriterFactory) BuildWriter() (io.WriteCloser, error) {
	var err error
	var num int
	var fname string

	for ; err == nil && num <= 999; num++ {
		fname = c.baseFilename + fmt.Sprintf(".%03d", num)
		_, err = os.Lstat(fname)
	}
	// return error if the last file checked still existed
	if err == nil {
		return nil, fmt.Errorf("Rotate: Cannot find free log number to rename %s\n", c.baseFilename)
	}

	fd, err := os.OpenFile(fname, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}
	gzWriter := gzip.NewWriter(fd)
	return &FileWriter{
		filename:   fname,
		file:       fd,
		gzipWriter: gzWriter,
		uploader:   c.uploader,
	}, nil
}

func (f *FileWriter) sendUploadRequest() {
	info, _ := os.Lstat(f.filename)
	if info.Size() > 30 {
		f.uploader.Upload(&uploader.UploadRequest{
			FileType: uploader.Gzip,
			Filename: f.filename,
		})
	} else {
		os.Remove(f.filename)
	}
}

func (f *FileWriter) Write(p []byte) (n int, err error) {
	return f.gzipWriter.Write(p)
}

func (f *FileWriter) Close() error {
	err := f.gzipWriter.Flush()
	if err != nil {
		return err
	}
	err = f.gzipWriter.Close()
	if err != nil {
		return err
	}

	err = f.file.Sync()
	if err != nil {
		return err
	}
	err = f.file.Close()
	if err != nil {
		return err
	}
	f.sendUploadRequest()
	return nil
}
