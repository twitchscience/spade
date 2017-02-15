package lookup

import (
	"io/ioutil"
	"net/http"
)

// HTTPRequestHandler is an interface to issue Get requests.
type HTTPRequestHandler interface {
	Get(string, map[string]string) ([]byte, error)
}

// BasicHTTPRequestHandler is an HTTPRequestHandler that uses an http.Client.
type BasicHTTPRequestHandler struct {
	HTTPClient *http.Client
}

// Get sends the given args to the given url and returns the response body or an error.
func (h *BasicHTTPRequestHandler) Get(url string, args map[string]string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	reqArgs := req.URL.Query()
	for k, v := range args {
		reqArgs.Set(k, v)
	}
	req.URL.RawQuery = reqArgs.Encode()

	resp, err := h.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}
