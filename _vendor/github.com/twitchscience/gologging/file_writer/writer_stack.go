package file_writer

import (
	"io"
	"log"
	"sync"
)

type WriterFactory interface {
	BuildWriter() (io.WriteCloser, error)
}

type WriterStack struct {
	CurrentWriter io.WriteCloser
	NextWriter    io.WriteCloser

	isNextWriterReady sync.WaitGroup
	writerBuilder     WriterFactory

	rotate    chan bool
	write     chan []byte
	Interrupt chan bool
}

func BuildWriterStack(writerBuilder WriterFactory, interruptChannel chan bool) (*WriterStack, error) {
	c, err := writerBuilder.BuildWriter()
	if err != nil {
		return nil, err
	}
	n, err := writerBuilder.BuildWriter()
	if err != nil {
		return nil, err
	}

	return &WriterStack{
		CurrentWriter:     c,
		NextWriter:        n,
		isNextWriterReady: sync.WaitGroup{},
		writerBuilder:     writerBuilder,
		rotate:            make(chan bool),
		write:             make(chan []byte),
		Interrupt:         interruptChannel,
	}, nil
}

func (s *WriterStack) Rotate() {
	s.rotate <- true
}

func (s *WriterStack) Write(msg []byte) {
	s.write <- msg
}

func (s *WriterStack) rotateNext() {
	// First, set currentwriter to next, save a reference to the currentWriter,
	// and in a separate thread we clean up that reference and build the nextWriter.
	lastWriter := s.CurrentWriter
	s.CurrentWriter = s.NextWriter

	// Potential race condition here if a rotate is triggered
	// before the next writer is repopulated.
	// Easily fixed with a waitgroup to force that only
	// one rotate is happening at a time.
	s.isNextWriterReady.Add(1)
	go func() {
		err := lastWriter.Close()
		if err != nil {
			log.Println("Got error closing the last writer: ", err)
		}

		s.NextWriter, err = s.writerBuilder.BuildWriter()
		if err != nil {
			log.Println("could not open the next writer ", err)
		}
		s.isNextWriterReady.Done()
	}()
}

func (s *WriterStack) writeToCurrent(msg []byte) (int, error) {
	return s.CurrentWriter.Write(msg)
}

func (s *WriterStack) loop() {
	for {
		select {
		case <-s.rotate:
			// We want to avoid blocking here. Whatever
			// triggers the rotate should use the rotate sparingly.
			s.isNextWriterReady.Wait()
			// This call swaps two pointers and forks so it should be quick.
			s.rotateNext()
		case msg, ok := <-s.write:
			if !ok {
				return
			}
			_, err := s.writeToCurrent(msg)
			if err != nil {
				log.Println("Got error writing: ", err)
			}
		case <-s.Interrupt:
			close(s.write)
		}
	}
}

func (s *WriterStack) Crank() {
	s.loop()
	// We need to wait here to ensure that the nextWriter can be closed.
	s.isNextWriterReady.Wait()
	err := s.CurrentWriter.Close()
	if err != nil {
		log.Println("Got error closing currentWriter: ", err)
	}

	err = s.NextWriter.Close()
	if err != nil {
		log.Println("Got error closing nextWriter: ", err)
	}

}
