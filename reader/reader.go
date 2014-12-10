package reader

import (
	"bufio"
	"compress/gzip"
	"os"
	"time"

	"github.com/twitchscience/spade/parser"
)

type EOF struct{}

func (e EOF) Error() string {
	return "Encountered an EOF"
}

type LogReader interface {
	ProvideLine() (parser.Parseable, error)
	Close() error
}

type FileLogReader struct {
	scanner *bufio.Scanner
	file    *os.File
	gzip    *gzip.Reader
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
	scanner, file, gzFile, err := getScanner(filename, useGzip)
	if err != nil {
		return &FileLogReader{scanner, file, gzFile}, err
	}
	return &FileLogReader{scanner, file, gzFile}, nil
}

func (reader *FileLogReader) Close() error {
	var err error = nil
	if reader.gzip != nil {
		err = reader.gzip.Close()
	}
	if reader.file != nil {
		err = reader.file.Close()
	}
	return err
}

func (reader *FileLogReader) ProvideLine() (parser.Parseable, error) {
	if reader.scanner.Scan() {
		return &parseRequest{[]byte(reader.scanner.Text()), time.Now()}, nil
	}
	err := reader.scanner.Err()
	if err != nil {
		return &parseRequest{nil, time.Now()}, err
	}
	return &parseRequest{nil, time.Now()}, EOF{}
}

func (reader *DummyLogReader) ProvideLine() (parser.Parseable, error) {
	if reader.timesRepeated <= reader.timesToRepeat {
		return &parseRequest{reader.logLine, time.Now()}, nil
	}
	return &parseRequest{nil, time.Now()}, EOF{}
}

func (reader *DummyLogReader) Close() error { return nil }

func getScanner(filename string, useGzip bool) (*bufio.Scanner, *os.File, *gzip.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, nil, err
	}

	var scanner *bufio.Scanner
	if useGzip {
		gzFile, gzErr := gzip.NewReader(file)
		if gzErr != nil {
			return nil, file, nil, gzErr
		}

		scanner = bufio.NewScanner(gzFile)
		return scanner, file, gzFile, nil
	}
	scanner = bufio.NewScanner(file)
	return scanner, file, nil, nil
}
