package gologging

const LogBufferLength = 4096

type LogRecord struct {
	Message string
}

type Logger interface {
	LogWrite(*LogRecord)
	Close()
}
