package reporter

// NoopReporter is a Reporter that does nothing.
type NoopReporter struct{}

// Record does nothing.
func (np NoopReporter) Record(*Result) {}

// IncrementExpected does nothing.
func (np NoopReporter) IncrementExpected(int) {}

// Reset does nothing.
func (np NoopReporter) Reset() {}

// Finalize returns an empty map.
func (np NoopReporter) Finalize() map[string]int {
	return make(map[string]int)
}
