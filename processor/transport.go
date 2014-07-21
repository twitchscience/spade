package processor

import (
	"bytes"
	"encoding/gob"
	"io"
	"sync"
)

type BufferedTransport struct {
	m   *sync.Mutex
	buf *bytes.Buffer
}

type GobTransport struct {
	Transport io.ReadWriteCloser
	Decoder   *gob.Decoder
	Encoder   *gob.Encoder
}

func NewBufferedTransport() *BufferedTransport {
	return &BufferedTransport{
		m:   &sync.Mutex{},
		buf: new(bytes.Buffer),
	}
}

func NewGobTransport(t io.ReadWriteCloser) *GobTransport {
	return &GobTransport{
		Transport: t,
		Decoder:   gob.NewDecoder(t),
		Encoder:   gob.NewEncoder(t),
	}
}

func (t *BufferedTransport) Read(b []byte) (n int, err error) {
	t.m.Lock()
	defer t.m.Unlock()

	return t.buf.Read(b)
}

func (t *BufferedTransport) Write(b []byte) (n int, err error) {
	t.m.Lock()
	defer t.m.Unlock()

	return t.buf.Write(b)
}

// TODO: decide how to handle closed traffix
func (t *BufferedTransport) Close() error {
	return nil
}

func (e *GobTransport) Write(v interface{}) error {

	return e.Encoder.Encode(v)
}

func (e *GobTransport) Read(v interface{}) error {
	return e.Decoder.Decode(v)
}

func (e *GobTransport) Close() error {
	return e.Transport.Close()
}
