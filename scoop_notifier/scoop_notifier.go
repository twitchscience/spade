package scoop_notifier

import (
	"bytes"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/TwitchScience/scoop_protocol/scoop_protocol"
)

const (
	bucket = "rsimport-oregon"
)

type ScoopClient struct {
	C      *http.Client
	Signer scoop_protocol.ScoopSigner
	Url    string
}

type ScoopRequest struct {
	Key       string
	Tablename string
	Result    chan error
}

func BuildScoopClient(url string) ScoopClient {
	// Think about proxys
	return ScoopClient{
		C:      &http.Client{},
		Signer: scoop_protocol.GetScoopSigner(),
		Url:    url,
	}
}

func condense(keys []string, resChannel chan error) []*ScoopRequest {
	var table, importKey string
	collector := make(map[string]bool)
	requests := make([]*ScoopRequest, 0, 32)
	for _, key := range keys {
		p := strings.Split(key, "/")
		importKey = strings.Join(p[:len(p)-1], "/")
		// p should be an array of 3 parts: the logname, the eventname, and the filepiece
		table = p[1]
		if ok := collector[importKey]; !ok {
			collector[importKey] = true
			requests = append(requests, &ScoopRequest{bucket + "/" + importKey + "/", table, resChannel})
		}
	}
	return requests
}

func (s *ScoopClient) Notify(keys []string, resChannel chan error) {
	// Need to take the keys and find common prefixes then group into tables
	for _, req := range condense(keys, resChannel) {
		log.Println(req)
		err := s.handle(req)
		if err != nil {
			log.Println(err)
		}
	}
}

func (s *ScoopClient) handle(req *ScoopRequest) error {
	reqS, err := s.Signer.SignJsonBody(scoop_protocol.RowCopyRequest{
		KeyName:   req.Key,
		TableName: req.Tablename,
	})
	if err != nil {
		return err
	}
	res, r_err := http.Post(s.Url, "application/json", bytes.NewReader(reqS))
	if r_err != nil {
		return r_err
	}
	if res.StatusCode != 204 {
		return errors.New("Server error: " + http.StatusText(res.StatusCode))
	}
	return nil
}

func (s *ScoopClient) SendCreateRequest(req scoop_protocol.Config) error {
	reqS, err := s.Signer.SignJsonBody(&req)
	if err != nil {
		return err
	}
	res, r_err := http.Post(s.Url, "application/json", bytes.NewReader(reqS))
	if r_err != nil {
		return r_err
	}
	if res.StatusCode != 204 {
		return errors.New("Server error: " + http.StatusText(res.StatusCode))
	}
	return nil
}
