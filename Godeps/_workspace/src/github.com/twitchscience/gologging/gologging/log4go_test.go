package gologging

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/twitchscience/aws_utils/uploader"
)

// This is the standard writer that prints to standard output.
type ConsoleLogWriter chan *LogRecord

type testNotifier struct {
	msg []string
}
type testNothingBuilder struct{}
type testUploader struct{}
type testErrorLogger struct{}

func (e *testErrorLogger) SendError(r error) {}

func (b *testNothingBuilder) BuildUploader() uploader.Uploader {
	return &testUploader{}
}

func (t *testUploader) GetKeyName(m string) string {
	return "Test"
}

func (t *testUploader) TargetLocation() string {
	return "Test"
}

func (t *testUploader) Upload(req *uploader.UploadRequest) (*uploader.UploadReceipt, error) {
	return &uploader.UploadReceipt{
		Path:    req.Filename,
		KeyName: t.GetKeyName(req.Filename),
	}, nil
}

func (t *testNotifier) SendMessage(req *uploader.UploadReceipt) error {
	t.msg = append(t.msg, req.Path)
	return nil
}

// This creates a new ConsoleLogWriter
func NewConsoleLogWriter(w io.Writer) ConsoleLogWriter {
	records := make(ConsoleLogWriter, LogBufferLength)
	go records.run(w)
	return records
}

func (w ConsoleLogWriter) run(out io.Writer) {
	for rec := range w {
		fmt.Fprint(out, rec.Message)
	}
}

// This is the ConsoleLogWriter's output method.  This will block if the output
// buffer is full.
func (w ConsoleLogWriter) LogWrite(rec *LogRecord) {
	w <- rec
}

// Close stops the logger from sending messages to standard output.  Attempts to
// send log messages to this logger after a Close have undefined behavior.
func (w ConsoleLogWriter) Close() {
	close(w)
	time.Sleep(50 * time.Millisecond) // Try to give console I/O time to complete
}
func TestLogger(t *testing.T) {
	var b bytes.Buffer
	log := NewConsoleLogWriter(&b)
	u := UploadLogger{
		Logger: log,
		Uploader: uploader.StartUploaderPool(
			2,
			&testErrorLogger{},
			&testNotifier{},
			&testNothingBuilder{},
		),
	}
	u.Log("%s %d %s", "Het", 54223, "Asdf")

	u.Close()
	if b.String() != "Het 54223 Asdf\n" {
		t.Logf("%s but got %s", "Het 54223 Asdf\n", b.String())
		t.Fail()
	}
}
