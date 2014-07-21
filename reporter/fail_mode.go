package reporter

type FailMode int

const (
	NONE FailMode = iota
	UNABLE_TO_PARSE_DATA
	NON_TRACKING_EVENT
	BAD_COLUMN_CONVERSION
	FAILED_WRITE
	EMPTY_REQUEST
	SKIPPED_COLUMN
	UNKNOWN_ERROR
	PANICED_IN_PROCESSING
	FAILED_TRANSPORT
)

func (m FailMode) String() string {
	switch m {
	case NONE:
		return "Success"
	case SKIPPED_COLUMN:
		return "Missing One or More columns"
	case UNABLE_TO_PARSE_DATA:
		return "Malformed data"
	case NON_TRACKING_EVENT:
		return "Untracked Event"
	case BAD_COLUMN_CONVERSION:
		return "Badly Typed Columns"
	case FAILED_WRITE:
		return "Failed To Write"
	case EMPTY_REQUEST:
		return "Empty Request"
	case UNKNOWN_ERROR:
		return "Unknown Failure"
	case PANICED_IN_PROCESSING:
		return "Paniced in Processing"
	case FAILED_TRANSPORT:
		return "Faild in transport"
	default:
		return "Unknown Failure"
	}
}
