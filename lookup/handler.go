package lookup

import (
	"io/ioutil"
	"net/http"
)

type HTTPRequestHandler interface {
	Get(string, map[string]string) ([]byte, error)
}

type BasicHTTPRequestHandler struct {
	HttpClient *http.Client
}

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

	resp, err := h.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}
