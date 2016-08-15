package fetcher

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// ConfigFetcher is responsible for obtaining and/or writing configs.
type ConfigFetcher interface {
	FetchAndWrite(io.ReadCloser, io.WriteCloser) error
	Fetch() (io.ReadCloser, error)
	ConfigDestination(string) (io.WriteCloser, error)
}

// FetchConfig fetches a config and writes it to the given filename.
func FetchConfig(cf ConfigFetcher, outputFileDst string) (err error) {
	src, err := cf.Fetch()
	if err != nil {
		return err
	}
	defer func() {
		if cerr := src.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	dest, err := cf.ConfigDestination(outputFileDst)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := dest.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	return cf.FetchAndWrite(src, dest)
}

type fetcher struct {
	hc  *http.Client
	url string
}

func timeoutDialer(timeout time.Duration) func(net, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(network, addr, timeout)
		if err != nil {
			return nil, err
		}
		if err = conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			return nil, err
		}
		return conn, nil
	}
}

// New returns a ConfigFetcher that reads configs from the given URL.
func New(url string) ConfigFetcher {
	return &fetcher{
		hc: &http.Client{
			Transport: &http.Transport{
				Dial: timeoutDialer(time.Duration(5) * time.Second),
			},
		},
		url: url,
	}
}

func validate(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	var cfgs []scoop_protocol.Config
	err := json.Unmarshal(b, &cfgs)
	return (err == nil)
}

// FetchAndWrite reads a config, validates it is a valid scoop_protocol.Config and writes it out.
func (f *fetcher) FetchAndWrite(src io.ReadCloser, dst io.WriteCloser) error {
	b, err := ioutil.ReadAll(src)
	if err != nil {
		return err
	}

	if ok := validate(b); !ok {
		return fmt.Errorf("Result not a valid []schema.Event: %s", string(b))
	}

	_, err = dst.Write(b)
	return err
}

// Fetch returns a reader for the config.
func (f *fetcher) Fetch() (io.ReadCloser, error) {
	resp, err := f.hc.Get(f.url)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// ConfigDestination returns a writer to the given path.
func (f *fetcher) ConfigDestination(outputFileName string) (io.WriteCloser, error) {
	return os.Create(outputFileName)
}
