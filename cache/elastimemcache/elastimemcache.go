package elastimemcache

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/aws/aws-sdk-go/service/elasticache/elasticacheiface"
	"github.com/bradfitz/gomemcache/memcache"

	"github.com/twitchscience/aws_utils/logger"
)

const (
	tickTime      = 10 * time.Minute
	retryDelay    = 2 * time.Second
	maxRetryDelay = 20 * time.Second
)

// A Config contains the parameters required by a Client to interact with a memcache cluster.
type Config struct {
	ClusterID string
	Namespace string
	TTL       int32
}

// A Client is a client for an ElastiCache cluster backed by memcache.
type Client struct {
	config         Config
	serverSelector *memcache.ServerList
	memcacheClient *memcache.Client
	awsClient      elasticacheiface.ElastiCacheAPI
	closer         chan bool
	rand           *rand.Rand
}

// NewClient returns a Client with a default aws session to interact with a elasticache cluster.
func NewClient(config Config) (*Client, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	return NewClientWithSession(sess, config)
}

// NewClientWithSession should be used if you want to override the elasticache instance with a
// non-default aws session.
func NewClientWithSession(session *session.Session, config Config) (*Client, error) {
	return NewClientWithInterface(elasticache.New(session), config)
}

// NewClientWithInterface returns a client instantiated from a custom elasticache client.
func NewClientWithInterface(
	awsClient elasticacheiface.ElastiCacheAPI,
	config Config,
) (*Client, error) {
	if awsClient == nil {
		return nil, fmt.Errorf("no elasticache interface was provided")
	}

	ss := &memcache.ServerList{}
	memcacheClient := memcache.NewFromSelector(ss)

	client := &Client{
		config:         config,
		serverSelector: ss,
		memcacheClient: memcacheClient,
		awsClient:      awsClient,
		closer:         make(chan bool),
		rand:           rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(os.Getpid()))),
	}
	err := client.updateNodesWithRetry(5)
	if err != nil {
		return nil, fmt.Errorf("initial node update: %v", err)
	}
	return client, nil
}

func (c *Client) createCacheKey(key string) string {
	return fmt.Sprintf("%s:%s", c.config.Namespace, key)
}

// randomJitter returns a uniformly random duration between t/2 and t.
func (c *Client) randomJitter(t time.Duration) time.Duration {
	return time.Duration(c.rand.Int63n(int64(t)/2) + int64(t)/2)
}

// updateNodes queries AWS to obtain a description of the cache cluster to extract and update
// the node endpoints in a thread-safe manner.
func (c *Client) updateNodes() error {
	resp, err := c.awsClient.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
		CacheClusterId:    aws.String(c.config.ClusterID),
		ShowCacheNodeInfo: aws.Bool(true),
	})
	if err != nil {
		return err
	}

	if len(resp.CacheClusters) != 1 {
		return fmt.Errorf("failed to find an unique cache cluster with id %s", c.config.ClusterID)
	}

	nodes := resp.CacheClusters[0].CacheNodes
	if len(nodes) == 0 {
		return fmt.Errorf("failed to find active nodes in the cache cluster with id %s",
			c.config.ClusterID)
	}

	//Construct slice with strings of address:port
	endpoints := make([]string, len(nodes))
	for i, node := range resp.CacheClusters[0].CacheNodes {
		if node == nil || node.Endpoint == nil || node.Endpoint.Address == nil ||
			node.Endpoint.Port == nil {
			// We have to be extra careful as a node info might be incomplete during changes
			continue
		}
		endpoints[i] = fmt.Sprintf("%s:%d", *node.Endpoint.Address, *node.Endpoint.Port)
	}

	return c.serverSelector.SetServers(endpoints...)
}

func (c *Client) updateNodesWithRetry(numRetries int) error {
	if numRetries <= 0 {
		return errors.New("numRetries is not positive")
	}

	retryWithBackoff := retryDelay
	var err error
	for i := 0; ; i++ {

		err = c.updateNodes()
		if err == nil {
			return nil
		}
		if i == numRetries {
			return fmt.Errorf("updating elasticache nodes: %v", err)
		}

		logger.WithError(err).WithField("window", retryWithBackoff).WithField("attempt", i).Warn("Failed to update nodes; retrying")
		time.Sleep(c.randomJitter(retryWithBackoff))
		retryWithBackoff *= 2
		if retryWithBackoff > maxRetryDelay {
			retryWithBackoff = maxRetryDelay
		}
	}
}

// StartAutoDiscovery is a blocking function that refreshes nodes on an interval.
func (c *Client) StartAutoDiscovery() {
	tick := time.After(c.randomJitter(tickTime))
	for {
		select {
		case <-c.closer:
			return
		case <-tick:
			tick = time.After(c.randomJitter(tickTime))
			if err := c.updateNodesWithRetry(2); err != nil {
				logger.WithError(err).Error("Failed to update nodes")
				continue
			}
		}
	}
}

// StopAutoDiscovery shuts down auto-discovery cleanly and blocks until it succeeds.
func (c *Client) StopAutoDiscovery() {
	c.closer <- true
}

// Get queries the cache for a string with the given key.
func (c *Client) Get(key string) (string, error) {
	item, err := c.memcacheClient.Get(c.createCacheKey(key))
	if err != nil {
		return "", err
	}
	return string(item.Value), nil
}

// Set inserts a string into the cache with the given key.
func (c *Client) Set(key string, value string) error {
	return c.memcacheClient.Set(&memcache.Item{
		Key:        c.createCacheKey(key),
		Value:      []byte(value),
		Expiration: c.config.TTL,
	})
}
