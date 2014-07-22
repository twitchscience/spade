package reader

import (
	"bufio"
	"compress/gzip"
	"github.com/twitchscience/spade/parser"
	"os"
	"time"
)

type EOF struct{}

func (e EOF) Error() string {
	return "Encountered an EOF"
}

type LogReader interface {
	ProvideLine() (*parser.ParseRequest, error)
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

func (reader *FileLogReader) ProvideLine() (*parser.ParseRequest, error) {
	if reader.scanner.Scan() {
		return &parser.ParseRequest{[]byte(reader.scanner.Text()), time.Now()}, nil
	}
	err := reader.scanner.Err()
	if err != nil {
		return &parser.ParseRequest{nil, time.Now()}, err
	}
	return &parser.ParseRequest{nil, time.Now()}, EOF{}
}

func (reader *DummyLogReader) ProvideLine() (*parser.ParseRequest, error) {
	if reader.timesRepeated <= reader.timesToRepeat {
		return &parser.ParseRequest{reader.logLine, time.Now()}, nil
	}
	return &parser.ParseRequest{nil, time.Now()}, EOF{}
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
