package key_name_generator

import (
	"crypto/rand"
	"fmt"
	"strings"
	"time"
)

type InstanceInfo struct {
	Service        string
	Cluster        string
	AutoScaleGroup string
	Node           string
	LoggingDir     string
}

// Fulfills the aws_utils/uploader.S3KeyNameGenerator interface.
type EdgeKeyNameGenerator struct {
	Info *InstanceInfo
}

// Fulfills the aws_utils/uploader.S3KeyNameGenerator interface.
type ProcessorKeyNameGenerator struct {
	Info *InstanceInfo
}

func (gen *EdgeKeyNameGenerator) GetKeyName(filename string) string {
	now := time.Now()
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%s/%s/%d.%s.%08x.log.gz",
		now.Format("20060102"),
		gen.Info.AutoScaleGroup,
		now.Unix(),
		gen.Info.Node,
		b,
	)
}

func (gen *ProcessorKeyNameGenerator) GetKeyName(filename string) string {
	now := time.Now()
	path := strings.LastIndex(filename, "/") + 1
	ext := strings.Index(filename, ".")
	if ext < 0 {
		ext = len(filename)
	}
	return fmt.Sprintf("%s/%s/%s/%s.%d.log.gz",
		now.Format("20060102"),
		filename[path:ext],
		gen.Info.AutoScaleGroup,
		gen.Info.Node,
		now.Unix(),
	)
}
