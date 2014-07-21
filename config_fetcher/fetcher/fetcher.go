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

	"github.com/TwitchScience/scoop_protocol/scoop_protocol"
)

type ConfigFetcher interface {
	FetchAndWrite(io.ReadCloser, io.WriteCloser) error
	Fetch() (io.ReadCloser, error)
	ConfigDestination(string) (io.WriteCloser, error)
}

func FetchConfig(cf ConfigFetcher, outputFileDst string) error {
	src, err := cf.Fetch()
	if err != nil {
		return err
	}
	defer src.Close()

	dest, err := cf.ConfigDestination(outputFileDst)
	if err != nil {
		return err
	}
	defer dest.Close()
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
		conn.SetDeadline(time.Now().Add(timeout))
		return conn, nil
	}
}

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

func (f *fetcher) FetchAndWrite(src io.ReadCloser, dst io.WriteCloser) error {
	b, err := ioutil.ReadAll(src)
	if err != nil {
		return err
	}

	if ok := validate(b); !ok {
		return fmt.Errorf("Result not a valid []scoop_protocol.Config: %s", string(b))
	}

	_, err = dst.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func (f *fetcher) Fetch() (io.ReadCloser, error) {
	resp, err := f.hc.Get(f.url)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (f *fetcher) ConfigDestination(outputFileName string) (io.WriteCloser, error) {
	return os.Create(outputFileName)
}
