package writer

import (
	"encoding/json"
	"time"

	"github.com/twitchscience/spade/reporter"
)

type WriteRequest struct {
	Category string
	Line     string
	UUID     string
	// Keep the source around for logging
	Source  json.RawMessage
	Failure reporter.FailMode
	Pstart  time.Time
}

func (r *WriteRequest) GetStartTime() time.Time {
	return r.Pstart
}

func (r *WriteRequest) GetCategory() string {
	return r.Category
}

func (r *WriteRequest) GetMessage() string {
	return string(r.Source)
}

func (r *WriteRequest) GetResult() *reporter.Result {
	return &reporter.Result{
		Failure:    r.Failure,
		UUID:       r.UUID,
		Line:       r.Line,
		Category:   r.Category,
		FinishedAt: time.Now(),
		Duration:   time.Now().Sub(r.Pstart),
	}
}
