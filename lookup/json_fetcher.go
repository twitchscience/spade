package lookup

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/elgs/gojq"
	"github.com/elgs/gosplitargs"
	"golang.org/x/time/rate"

	"github.com/twitchscience/spade/reporter"
)

// JSONValueFetcherConfig contains the required information to instantiate a JSONValueFetcher
type JSONValueFetcherConfig struct {
	URL           string // URL to send the GET request
	JQ            string // JQ expression to extract a particular field from the fetched JSON
	TimeoutMS     int    // Timeout in ms for GET requests
	RatePerSecond int    // Amount of requests allowed per second
	BurstSize     int    // Maximum amount of concurrent requests possible
}

// JSONValueFetcher extracts a particular field value from a JSON obtained through a GET request
type JSONValueFetcher struct {
	config  JSONValueFetcherConfig
	handler HTTPRequestHandler
	limiter *rate.Limiter
	stats   reporter.StatsLogger
}

// validateJQ returns an error in the event the provided JQ expression is not valid for gojq parsing
func validateJQ(exp string) error {
	// we can only really validate that the expression can be parsed safely later, but we have no
	// way of knowing if the query is truly valid since it depends on the json
	if exp == "." {
		return nil
	}
	paths, err := gosplitargs.SplitArgs(exp, "\\.", false)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if len(path) >= 3 && strings.HasPrefix(path, "[") && strings.HasSuffix(path, "]") {
			// validate we have an array index
			_, err := strconv.Atoi(path[1 : len(path)-1])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// NewJSONValueFetcher returns a *JSONValueFetcher that uses a BasicHTTPRequestHandler wrapping a
// http.Client instance with the given timeout in ms to service GET requests
func NewJSONValueFetcher(
	config JSONValueFetcherConfig,
	stats reporter.StatsLogger,
) (*JSONValueFetcher, error) {
	if config.TimeoutMS < 1 {
		return nil, errors.New("TimeoutMS needs to be a positive integer")
	}
	if config.RatePerSecond < 1 {
		return nil, errors.New("RatePerSecond needs to be a positive integer")
	}
	err := validateJQ(config.JQ)
	if err != nil {
		return nil, err
	}
	handler := BasicHTTPRequestHandler{&http.Client{
		Timeout: time.Duration(config.TimeoutMS) * time.Millisecond,
	}}
	return &JSONValueFetcher{
		config:  config,
		handler: &handler,
		limiter: rate.NewLimiter(rate.Limit(config.RatePerSecond), config.BurstSize),
		stats:   stats,
	}, nil
}

// fetchHelper is a thread-safe function that invokes a GET request to obtain the JSON and returns
// a JQ instance for querying. Internally, this function will limit the rate of requests per second
// to whatever is defined by RatePerSecond. In the event the rate is exceeded, this function will
// immediately return with ErrTooManyRequests.
func (f *JSONValueFetcher) fetchHelper(args map[string]string) (*gojq.JQ, error) {
	if !f.limiter.Allow() {
		return nil, ErrTooManyRequests
	}

	b, err := f.handler.Get(f.config.URL, args)
	if err != nil {
		return nil, err
	}
	f.stats.IncrBy("fetcher.json.requests", 1)

	var jsonBlob interface{}
	if err := json.Unmarshal(b, &jsonBlob); err != nil {
		return nil, err
	}
	return gojq.NewQuery(jsonBlob), nil
}

// FetchInt64 constructs a GET HTTP query with the provided map as URL arguments and returns the value
// as an int64 if possible
func (f *JSONValueFetcher) FetchInt64(args map[string]string) (int64, error) {
	parser, err := f.fetchHelper(args)
	if err != nil {
		return 0, err
	}
	value, err := parser.QueryToInt64(f.config.JQ)
	if err != nil {
		return 0, ErrExtractingValue
	}
	return value, nil
}
