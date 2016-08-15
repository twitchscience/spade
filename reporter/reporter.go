package reporter

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/gologging/gologging"
)

const reportBuffer = 400000

// StatsLogger is an interface to receive statsd stats.
type StatsLogger interface {
	Timing(string, time.Duration)
	IncrBy(string, int)
}

// Reporter Records Results and returns stats on them.
type Reporter interface {
	Record(*Result)
	IncrementExpected(int)
	Reset()
	Finalize() map[string]int
}

// Tracker Tracks Results in some external system.
type Tracker interface {
	Track(*Result)
}

// Result tracks an input record with identifiers, timing information, and failures.
type Result struct {
	Duration   time.Duration
	FinishedAt time.Time
	Failure    FailMode
	UUID       string
	Line       string
	Category   string
}

// SpadeReporter is a Reporter that also sends stats to external Trackers.
// It is NOT thread safe.
type SpadeReporter struct {
	Wait     *sync.WaitGroup
	Trackers []Tracker
	Stats    map[string]int
	record   chan *Result
	report   chan chan map[string]int
	reset    chan bool
}

// SpadeStatsdTracker is a Tracker that reports to statsd.
type SpadeStatsdTracker struct {
	Stats StatsLogger
}

// SpadeUUIDTracker is a Tracker that reports to UUIDs to the spade audit log.
type SpadeUUIDTracker struct {
	Logger *gologging.UploadLogger
}

// spadeAuditLog defines a struct of spade audit log to output as json
// any change to this struct, please make relevant changes to
// func (s *SpadeUUIDTracker) Track(result *Result) and
// update the explicit string printing given marshall error
type spadeAuditLog struct {
	UUID       string
	FinishedAt time.Time
	Duration   float64
	Failure    string
}

// BuildSpadeReporter builds a SpadeReporter on the given WaitGroup and Trackers.
func BuildSpadeReporter(wait *sync.WaitGroup, trackers []Tracker) Reporter {
	r := &SpadeReporter{
		Wait:     wait,
		Trackers: trackers,
		Stats:    make(map[string]int),
		record:   make(chan *Result, reportBuffer),
		report:   make(chan chan map[string]int),
		reset:    make(chan bool),
	}
	logger.Go(r.crank)
	return r
}

func (r *SpadeReporter) crank() {
	for {
		select {
		case result := <-r.record:
			for _, t := range r.Trackers {
				t.Track(result)
			}
			r.Stats[result.Failure.String()]++
			r.Wait.Done()
		case responseChan := <-r.report:
			c := make(map[string]int, len(r.Stats))
			for k, v := range r.Stats {
				c[k] = v
			}
			responseChan <- c
		case <-r.reset:
			for k := range r.Stats {
				delete(r.Stats, k)
			}
		}
	}
}

// IncrementExpected adds the given number to the WaitGroup.
func (r *SpadeReporter) IncrementExpected(n int) {
	r.Wait.Add(n)
}

// Record sends the given result to all trackers and our stats report.
func (r *SpadeReporter) Record(result *Result) {
	r.record <- result
}

// Report returns the current stats report.
func (r *SpadeReporter) Report() map[string]int {
	responseChan := make(chan map[string]int)
	defer close(responseChan)
	r.report <- responseChan
	return <-responseChan
}

// Finalize waits for processing to finish and returns a copy of the reporter's stat map.
// Be sure to call Finalize only after all lines are parsed from log.
func (r *SpadeReporter) Finalize() map[string]int {
	r.Wait.Wait()
	return r.Report()
}

// Reset resets our stat map.
func (r *SpadeReporter) Reset() {
	r.reset <- true
}

// Track sends the given result to statsd.
func (s *SpadeStatsdTracker) Track(result *Result) {
	if result.Failure == None || result.Failure == SkippedColumn {
		s.Stats.IncrBy(fmt.Sprintf("%s.success", result.Category), 1)
	} else {
		s.Stats.IncrBy(fmt.Sprintf("%s.fail", result.Category), 1)
	}
	s.Stats.Timing(fmt.Sprintf("%d", result.Failure), result.Duration)
}

// Track sends the result's UUID, timing, and error to the spade audit logger.
func (s *SpadeUUIDTracker) Track(result *Result) {
	newAuditLog := spadeAuditLog{
		UUID:       result.UUID,
		FinishedAt: result.FinishedAt,
		Duration:   result.Duration.Seconds(),
		Failure:    result.Failure.String(),
	}
	jsonBytes, err := json.Marshal(newAuditLog)
	if err != nil { //write json string explicitlt given any marshal error
		s.Logger.Log(fmt.Sprintf("{\"UUID\":\"%s\", \"FinishedAt\":\"%v\", \"Duration\":%f, \"Failure\":\"%s\"}",
			result.UUID, result.FinishedAt, result.Duration.Seconds(), result.Failure.String()))
	}
	s.Logger.Log(string(jsonBytes))
}
