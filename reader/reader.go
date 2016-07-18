package reader

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/spade/parser"
)

type LogReader interface {
	ProvideLine() (parser.Parseable, error)
	Close() error
}

type FileLogReader struct {
	reader       *bufio.Reader
	decompressor io.ReadCloser
	file         io.Closer
	endOfFile    bool
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

	if reader.decompressor != nil {
		err = reader.decompressor.Close()
	}

	if reader.file != nil {
		internalErr := reader.file.Close()
		if internalErr != nil {
			if err == nil {
				err = internalErr
			} else {
				logger.WithError(internalErr).Error("Swallowed while closing file")
			}
		}
	}
	return err
}

func (reader *FileLogReader) ProvideLine() (parser.Parseable, error) {
	if reader.endOfFile {
		return nil, io.EOF
	}
	line, err := reader.reader.ReadBytes('\n')

	// Emulate the scanner behavior of only returning EndOfFile after
	// getting the last line
	if err == io.EOF {
		reader.endOfFile = true
		err = nil
	}

	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	return &parseRequest{line, time.Now()}, err
}

func (reader *DummyLogReader) ProvideLine() (parser.Parseable, error) {
	if reader.timesRepeated <= reader.timesToRepeat {
		return &parseRequest{reader.logLine, time.Now()}, nil
	}
	return &parseRequest{nil, time.Now()}, io.EOF
}

func (reader *DummyLogReader) Close() error { return nil }
