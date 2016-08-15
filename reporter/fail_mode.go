package reporter

// FailMode is an enum of the types of failure modes we report.
type FailMode int

// See String() for a more verbose explanation of each.
const (
	None FailMode = iota
	UnableToParseData
	NonTrackingEvent
	BadColumnConversion
	FailedWrite
	EmptyRequest
	SkippedColumn
	UnknownError
	PanickedInProcessing
	FailedTransport
)

// Return a human-readable string describing the given FailMode.
func (m FailMode) String() string {
	switch m {
	case None:
		return "Success"
	case SkippedColumn:
		return "Missing One or More Columns"
	case UnableToParseData:
		return "Malformed Data"
	case NonTrackingEvent:
		return "Untracked Event"
	case BadColumnConversion:
		return "Badly Typed Columns"
	case FailedWrite:
		return "Failed To Write"
	case EmptyRequest:
		return "Empty Request"
	case UnknownError:
		return "Unknown Failure"
	case PanickedInProcessing:
		return "Panicked in Processing"
	case FailedTransport:
		return "Failed in Transport"
	default:
		return "Unknown Failure"
	}
}
