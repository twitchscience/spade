package reader

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"time"

	"github.com/twitchscience/spade/parser"
)

const maxLineLength = 512 * 1024 * 1024

type SkipLongLine struct {}
func (e SkipLongLine) Error() string {
	return "Line longer than max, skipping"
}


type LogReader interface {
	ProvideLine() (parser.Parseable, error)
	Close() error
}

type FileLogReader struct {
	reader       *bufio.Reader
	decompressor io.ReadCloser
	file         io.Closer
}

type DummyLogReader struct {
	logLine       []byte
	timesToRepeat uint
	timesRepeated uint
}

type parseRequest struct {
	data  []byte
	start time.Time
}

func (p *parseRequest) Data() []byte {
	return p.data
}

func (p *parseRequest) StartTime() time.Time {
	return p.start
}

func GetFileLogReader(filename string, useGzip bool) (*FileLogReader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	var decompressor *gzip.Reader
	var reader *bufio.Reader

	if useGzip {
		decompressor, err = gzip.NewReader(file)
		if err == nil {
			reader = bufio.NewReader(decompressor)
		}
	} else {
		reader = bufio.NewReader(file)
	}

	if err != nil {
		if file != nil {
			file.Close()
		}
		if decompressor != nil {
			decompressor.Close()
		}
		return nil, err
	}

	return &FileLogReader{
		reader:       reader,
		file:         file,
		decompressor: decompressor}, nil
}

func (reader *FileLogReader) Close() error {
	var err error

	// It's possible to lose errors here, we will return
	// the first we find but still keep closing to
	// reduce leakage
	if reader.decompressor != nil {
		err = reader.decompressor.Close()
	}

	if reader.file != nil {
		internalErr := reader.file.Close()
		if internalErr != nil && err == nil {
			err = internalErr
		}
	}
	return err
}

func (reader *FileLogReader) ProvideLine() (parser.Parseable, error) {
	line, err := reader.reader.ReadBytes('\n')
	return &parseRequest{line, time.Now()}, err
}

func (reader *DummyLogReader) ProvideLine() (parser.Parseable, error) {
	if reader.timesRepeated <= reader.timesToRepeat {
		return &parseRequest{reader.logLine, time.Now()}, nil
	}
	return &parseRequest{nil, time.Now()}, io.EOF
}

func (reader *DummyLogReader) Close() error { return nil }
