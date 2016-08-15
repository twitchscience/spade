package globber

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

type dummy struct {
	I int
	S string
}

func TestInvalidConfig(t *testing.T) {
	validConfig := Config{
		MaxSize:      1 * 1024 * 1024,
		MaxAge:       "1m",
		BufferLength: 5,
	}
	assert.NoError(t, validConfig.Validate())

	config := validConfig
	config.MaxSize = 0
	assert.Error(t, config.Validate())

	config = validConfig
	config.MaxAge = "foo"
	assert.Error(t, config.Validate())

	config = validConfig
	config.MaxAge = "0"
	assert.Error(t, config.Validate())

	config = validConfig
	config.BufferLength = 0
	assert.Error(t, config.Validate())
}

func decompress(b []byte) ([]byte, error) {
	compressed := bytes.NewBuffer(b)

	v, err := compressed.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Error reading version byte: %s", err)
	}
	if v != version {
		return nil, fmt.Errorf("Unknown version, got %v expected %v", v, version)
	}

	deflator := flate.NewReader(compressed)
	defer func() {
		_ = deflator.Close()
	}()

	var decompressed bytes.Buffer
	_, err = io.Copy(&decompressed, deflator)
	if err != nil {
		return nil, fmt.Errorf("Error decompressiong: %v", err)
	}
	return decompressed.Bytes(), nil
}

func TestGlobber(t *testing.T) {
	data := []map[string]string{
		map[string]string{
			"one":  "english",
			"zwei": "german",
			"trzy": "polish",
		},
		map[string]string{
			"oink": "pig",
			"moo":  "cow",
			"bark": "tree",
		},
	}
	expected, _ := json.Marshal(data)

	// config globber to be much bigger than
	// we need
	config := Config{
		MaxSize:      1 * 1024 * 1024,
		MaxAge:       "1m",
		BufferLength: 5,
	}

	var result []byte

	g, err := New(config, func(b []byte) {
		r, e := decompress(b)
		assert.NoError(t, e)
		result = r
	})

	assert.NoError(t, err)
	for _, e := range data {
		err := g.Submit(e)
		assert.NoError(t, err)
	}
	g.Close()

	assert.EqualValues(t, expected, result)
}
