package lookup

// ValueFetcher fetches values with a given set of arguments.
type ValueFetcher interface {
	FetchInt64(map[string]string) (int64, error)
}
