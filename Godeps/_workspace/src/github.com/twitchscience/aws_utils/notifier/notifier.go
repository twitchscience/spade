package notifier

import (
	"crypto/md5"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/twitchscience/aws_utils/common"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"
)

func TimeoutDialer(cTimeout time.Duration, rwTimeout time.Duration) func(net, addr string) (c net.Conn, err error) {
	return func(netw, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(netw, addr, cTimeout)
		if err != nil {
			return nil, err
		}
		conn.SetDeadline(time.Now().Add(rwTimeout))
		return conn, nil
	}
}

func NewTimeoutClient(connectTimeout time.Duration, readWriteTimeout time.Duration) *http.Client {
	http.DefaultClient.Transport = &http.Transport{
		Dial: TimeoutDialer(connectTimeout, readWriteTimeout),
	}
	return http.DefaultClient
}

var (
	client        = NewTimeoutClient(10*time.Second, 10*time.Second)
	DefaultClient = BuildSQSClient(GetAuthFromEnv(), aws.USWest2)
	Retrier       = &common.Retrier{
		Times:         3,
		BackoffFactor: 2,
	}
)

func GetAuthFromEnv() aws.Auth {
	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil && os.Getenv("CLOUD_ENVIRONMENT") == "PRODUCTION" {
		log.Fatalln("Failed to recieve auth from env")
	}
	return auth
}

type MessageCreator map[string]func(...interface{}) (string, error)

type SQSClient struct {
	Signer *MessageCreator
	SQS    *sqs.SQS
}

func BuildSQSClient(auth aws.Auth, region aws.Region) *SQSClient {
	m := make(MessageCreator)
	m.RegisterMessageType("error", func(args ...interface{}) (string, error) {
		return fmt.Sprintf("%v", args...), nil
	})
	return &SQSClient{
		Signer: &m,
		SQS:    sqs.New(auth, region),
	}
}

func (s *SQSClient) GetAndCreateQIfNotExist(qName string, timeout int) (*sqs.Queue, error) {
	var q *sqs.Queue
	err := Retrier.Retry(func() error {
		var e error
		q, e = s.SQS.GetQueue(qName)
		return e
	})
	if err != nil {
		q, err = s.SQS.CreateQueueWithTimeout(qName, timeout)
	}
	return q, err
}

func (s *SQSClient) SendMessage(messageType, qName string, args ...interface{}) error {
	q, err := s.GetAndCreateQIfNotExist(qName, 300)
	if err != nil {
		return err
	}
	message, err := s.Signer.SignBody(messageType, args...)
	if err != nil {
		return err
	}
	return s.handle(message, q)
}

func (s *SQSClient) handle(message string, q *sqs.Queue) error {
	var res *sqs.SendMessageResponse
	err := Retrier.Retry(func() error {
		// Call sqs.Queue.SendMessage() - initiates a HTTP request.
		var e error
		res, e = q.SendMessage(message)
		return e
	})
	if err != nil {
		return err
	}
	// check md5
	expectedHash := md5.New()
	expectedHash.Write([]byte(message))
	expected := fmt.Sprintf("%x", expectedHash.Sum(nil))
	if expected != res.MD5 {
		return errors.New(fmt.Sprintf("message %s Did Not match expected %s", res.MD5, expected))
	}
	return nil
}

func (m *MessageCreator) RegisterMessageType(name string, messageType func(...interface{}) (string, error)) {
	(*m)[name] = messageType
}

func (m *MessageCreator) SignBody(messageType string, args ...interface{}) (string, error) {
	if fn, ok := (*m)[messageType]; ok {
		return fn(args...)
	} else {
		return "", errors.New("message" + messageType + "does not exists")
	}
}
