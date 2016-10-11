package gologging

import (
	"fmt"
	"time"

	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/gologging/file_writer"
	gen "github.com/twitchscience/gologging/key_name_generator"
)

type RotateCoordinator struct {
	MaxLines         int
	MaxTime          time.Duration
	currentLines     int
	timeAtLastRotate time.Time
}

// This log writer sends output to a file
type FileLogS3Writer struct {
	Interrupt      chan bool
	stackInterrupt chan bool
	stackDone      chan bool
	rec            chan *LogRecord

	coordinator *RotateCoordinator

	writer *file_writer.WriterStack
}

func NewRotateCoordinator(maxLines int, maxTime time.Duration) *RotateCoordinator {
	return &RotateCoordinator{
		MaxLines:         maxLines,
		MaxTime:          maxTime,
		currentLines:     0,
		timeAtLastRotate: time.Now(),
	}
}

// Updates the RotateCoordinator and returns true if rotate conditions are met.
func (r *RotateCoordinator) TestForRotate() bool {
	// update testers state
	now := time.Now()
	if r.currentLines > r.MaxLines || now.Sub(r.timeAtLastRotate) > r.MaxTime {
		r.currentLines = 0
		r.timeAtLastRotate = now
		return true
	}
	return false
}

func (r *RotateCoordinator) Update() {
	r.currentLines++
}

// This is the FileLogS3Writer's output method
func (w *FileLogS3Writer) LogWrite(rec *LogRecord) {
	w.rec <- rec
}

func (w *FileLogS3Writer) Close() {
	w.Interrupt <- true
	// Tell the stack to clean up and wait until the stack is complete.
	<-w.stackDone
}

func StartS3LogWriter(uploader *uploader.UploaderPool, info *gen.InstanceInfo, coordinator *RotateCoordinator) (*FileLogS3Writer, error) {
	filename := fmt.Sprintf("%s/%s.log.gz", info.LoggingDir, info.Service)
	stackInterrupt := make(chan bool)
	interrupt := make(chan bool)
	writeStack, err := file_writer.BuildWriterStack(
		file_writer.BuildFileWriterFactory(filename, uploader),
		stackInterrupt,
	)
	if err != nil {
		return nil, err
	}
	stackFinished := make(chan bool)
	go func() {
		writeStack.Crank()
		stackFinished <- true
	}()
	w := &FileLogS3Writer{
		writer: writeStack,

		rec:            make(chan *LogRecord, LogBufferLength),
		stackInterrupt: stackInterrupt,
		stackDone:      stackFinished,
		Interrupt:      interrupt,
		coordinator:    coordinator,
	}

	go func() {
		w.loop()
		w.stackInterrupt <- true
	}()

	return w, nil
}

func (w *FileLogS3Writer) loop() {
	for {
		select {
		case rec, ok := <-w.rec:
			if !ok {
				return
			}
			if w.coordinator.TestForRotate() {
				w.writer.Rotate() // needs to send a upload request
			}

			// Perform the write
			w.writer.Write([]byte(rec.Message))
			w.coordinator.Update()
		case <-w.Interrupt:
			close(w.rec)
		}
	}
}
