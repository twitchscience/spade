package globber

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/logger"
)

var (
	prefix         = '['
	separator      = ','
	postfix        = ']'
	version   byte = 1
)

// Complete is the type of a function that Globber will
// call for every completed glob
type Complete func([]byte)

// Config is used to configure a globber instance
type Config struct {
	// MaxSize is the max size per glob before compression
	MaxSize int

	// MaxAge is the max age of the oldest entry in the glob
	MaxAge string

	// BufferLength is the length of the channel where newly
	// submitted entries are stored, decreasing the size of this
	// buffer can cause stalls, and increasing the size can increase
	// shutdown time
	BufferLength int
}

func (c *Config) Validate() error {
	maxAge, err := time.ParseDuration(c.MaxAge)
	if err != nil {
		return err
	}

	if maxAge <= 0 {
		return errors.New("MaxAge must be a positive value")
	}

	if c.MaxSize <= 0 {
		return errors.New("MaxSize must be a positive value")
	}

	if c.BufferLength == 0 {
		return errors.New("BufferLength must be a positive value")
	}

	return nil
}

// A Globber is an object that will combine a bunch of json marshallable
// objects into compressed json array
type Globber struct {
	config     Config
	completor  Complete
	compressor *flate.Writer
	incoming   chan []byte
	pending    bytes.Buffer
	timer      *time.Timer
	maxAge     time.Duration

	sync.WaitGroup
}

// New returns a newly created instance of a Globber
func New(config Config, completor Complete) (*Globber, error) {
	err := config.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid config: %s", err)
	}
	maxAge, _ := time.ParseDuration(config.MaxAge)

	g := &Globber{
		config:    config,
		completor: completor,
		maxAge:    maxAge,
		timer:     time.NewTimer(maxAge),
		incoming:  make(chan []byte, config.BufferLength),
	}

	g.Add(1)
	logger.Go(g.worker)
	return g, nil
}

// Submit submits an object for globbing
func (g *Globber) Submit(entry interface{}) error {
	e, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %v", err)
	}

	g.incoming <- e
	return nil
}

// Close stops the globbing process. Will return after all
// entries are flushed
func (g *Globber) Close() {
	close(g.incoming)
	g.Wait()
}

func (g *Globber) add(entry []byte) {
	s := len(entry) + g.pending.Len()
	if s > g.config.MaxSize {
		g.complete()
	}

	if g.pending.Len() == 0 {
		g.timer.Reset(g.maxAge)
		g.pending.WriteRune(prefix)
	} else {
		g.pending.WriteRune(separator)
	}
	g.pending.Write(entry)
}

func (g *Globber) complete() {
	if g.pending.Len() == 0 {
		return
	}

	g.pending.WriteRune(postfix)
	err := g._complete()
	if err != nil {
		logger.WithError(err).Error("Failed to compress glob")
	}
}

func (g *Globber) _complete() error {
	var compressed bytes.Buffer

	err := compressed.WriteByte(version)
	if err != nil {
		return err
	}

	if g.compressor == nil {
		g.compressor, err = flate.NewWriter(&compressed, flate.BestSpeed)
		if err != nil {
			return err
		}
	} else {
		g.compressor.Reset(&compressed)
	}
	_, err = g.compressor.Write(g.pending.Bytes())
	if err != nil {
		return err
	}

	err = g.compressor.Close()
	if err != nil {
		return err
	}

	g.completor(compressed.Bytes())
	g.pending.Reset()
	return nil
}

func (g *Globber) worker() {
	defer g.Done()
	defer g.complete()
	for {
		select {
		case <-g.timer.C:
			g.complete()
		case e, ok := <-g.incoming:
			if !ok {
				return
			}
			g.add(e)
		}
	}
}
