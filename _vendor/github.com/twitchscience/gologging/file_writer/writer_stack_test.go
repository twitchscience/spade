package file_writer

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
	"time"
)

var (
	keeper            = new(bytes.Buffer)
	buffersDumped     = 0
	expectedOutput, _ = ioutil.ReadFile("test_resources/expected_output.txt")
)

type bufferWriterConstructor struct{}

func (b *bufferWriterConstructor) BuildWriter() (io.WriteCloser, error) {
	return new(byteBufferCloser), nil
}

type byteBufferCloser struct {
	buf bytes.Buffer
}

func (b *byteBufferCloser) Close() error {
	keeper.WriteString(fmt.Sprintf("+++++\nDumping Buffer #%d\n", buffersDumped))
	b.buf.WriteTo(keeper)
	buffersDumped++
	return nil

}

func (b *byteBufferCloser) Write(p []byte) (int, error) {
	return b.buf.Write(p)
}

func TestWriterStack(t *testing.T) {
	interruptChannel := make(chan bool)
	writerConstructor := new(bufferWriterConstructor)
	stack, err := BuildWriterStack(writerConstructor, interruptChannel)
	go stack.Crank()
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < 4; i++ {
		for j := 0; j < 10; j++ {
			stack.Write([]byte(fmt.Sprintf("test msg# %d for buffer# %d\n", j, i)))
		}
		stack.Rotate()
	}
	interruptChannel <- true
	time.Sleep(5 * time.Millisecond)
	// Put a sleep to allow the crank to exit
	var output []byte
	n, _ := keeper.Read(output)
	if bytes.Equal(output, expectedOutput) {
		t.Errorf("expected %s but got %s\n", output[:n], expectedOutput)
	}
}
