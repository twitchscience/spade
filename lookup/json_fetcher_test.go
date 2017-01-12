package lookup

import (
	"sync"
	"testing"

	"golang.org/x/time/rate"
)

const (
	ratePS = 3
)

var (
	jsonConfig = JSONValueFetcherConfig{
		URL:           "<whatever_is_not_used>",
		JQ:            "results.[0].id",
		TimeoutMS:     -1,
		RatePerSecond: ratePS,
		BurstSize:     ratePS,
	}
)

type DummyHTTPRequestHandler struct {
	JSON []byte
}

func (h *DummyHTTPRequestHandler) Get(url string, args map[string]string) ([]byte, error) {
	return h.JSON, nil
}

func newFetcher(handler HTTPRequestHandler) JSONValueFetcher {
	return JSONValueFetcher{
		config:  jsonConfig,
		handler: handler,
		limiter: rate.NewLimiter(rate.Limit(jsonConfig.RatePerSecond), jsonConfig.BurstSize),
	}
}

func TestValidJSONValueFetch(t *testing.T) {
	handler := &DummyHTTPRequestHandler{
		JSON: []byte(`{"results": [{"id": 96046250,"login": "cado"}]}`),
	}
	fetcher := newFetcher(handler)
	args := map[string]string{
		"login": "cado",
	}
	value, err := fetcher.FetchInt64(args)
	if err != nil {
		t.Fatalf("Failed to fetch value with error '%v'", err)
	}
	if value != 96046250 {
		t.Fatalf("Fetched value is incorrect: Expected 96046250, but received %v", value)
	}
}

func TestInvalidJSONValueFetch(t *testing.T) {
	handler := &DummyHTTPRequestHandler{
		JSON: []byte(`{"results": [{"id": 96046250,"login": "cado"}}`),
	}
	fetcher := newFetcher(handler)
	args := map[string]string{
		"login": "cado",
	}
	_, err := fetcher.FetchInt64(args)
	if err == nil {
		t.Fatal("No failure when trying to fetch a value from an invalid JSON")
	}
}

func TestInvalidFieldJSONValueFetch(t *testing.T) {
	handler := &DummyHTTPRequestHandler{
		JSON: []byte(`{"results": [{"login": "cado"}]}`),
	}
	fetcher := newFetcher(handler)
	args := map[string]string{
		"login": "cado",
	}
	_, err := fetcher.FetchInt64(args)
	if err == nil {
		t.Fatal("No failure when trying to fetch a value with invalid field 'id'")
	}
}

func TestMultipleValidJSONValueFetches(t *testing.T) {
	handler := &DummyHTTPRequestHandler{
		JSON: []byte(`{"results": [{"id": 96046250,"login": "cado"}]}`),
	}
	fetcher := newFetcher(handler)
	args := map[string]string{
		"login": "cado",
	}

	var wg sync.WaitGroup
	errors := make([]error, ratePS)
	wg.Add(ratePS)
	for i := 0; i < ratePS; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, errors[i] = fetcher.FetchInt64(args)
		}()
	}
	wg.Wait()
	for i := 0; i < ratePS; i++ {
		if errors[i] != nil {
			t.Fatalf("Failure when sending multiple requests: %v", errors[i])
		}
	}
}

func TestTooManyJSONValueFetches(t *testing.T) {
	handler := &DummyHTTPRequestHandler{
		JSON: []byte(`{"results": [{"id": 96046250,"login": "cado"}]}`),
	}
	fetcher := newFetcher(handler)
	args := map[string]string{
		"login": "cado",
	}

	var wg sync.WaitGroup
	largeRatePS := ratePS * 2
	errors := make([]error, largeRatePS)
	wg.Add(largeRatePS)
	for i := 0; i < largeRatePS; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, errors[i] = fetcher.FetchInt64(args)
		}()
	}
	wg.Wait()
	foundError := false
	for i := 0; i < largeRatePS; i++ {
		if errors[i] == ErrTooManyRequests {
			foundError = true
			break
		}
	}
	if !foundError {
		t.Fatalf("No error was found when sending too many requests")
	}
}
