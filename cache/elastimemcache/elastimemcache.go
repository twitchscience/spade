package elastimemcache

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/aws/aws-sdk-go/service/elasticache/elasticacheiface"
	"github.com/bradfitz/gomemcache/memcache"

	"github.com/twitchscience/aws_utils/logger"
)

const (
	tickTime   = time.Duration(1) * time.Minute
	retryDelay = time.Duration(30) * time.Second
)

// Config contains the parameters required by a Client to interact with a memcache cluster.
type Config struct {
	ClusterId string
	Namespace string
	TTL       int32
}

type Client struct {
	config         Config
	serverSelector *memcache.ServerList
	memcacheClient *memcache.Client
	awsClient      elasticacheiface.ElastiCacheAPI
	closer         chan bool
}

// NewClient returns a Client with a default aws session to interact with a elasticache cluster.
func NewClient(config Config) (*Client, error) {
	return NewClientWithSession(session.New(), config)
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
	}
	err := client.updateNodesWithRetry(5)
	return client, err
}

func (c *Client) createCacheKey(key string) string {
	return fmt.Sprintf("%s:%s", c.config.Namespace, key)
}

// updateNodes queries AWS to obtain a description of the cache cluster to extract and update
// the node endpoints in a thread-safe manner.
func (c *Client) updateNodes() error {
	resp, err := c.awsClient.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
		CacheClusterId:    aws.String(c.config.ClusterId),
		ShowCacheNodeInfo: aws.Bool(true),
	})
	if err != nil {
		return err
	}

	if len(resp.CacheClusters) != 1 {
		return fmt.Errorf("failed to find an unique cache cluster with id %s", c.config.ClusterId)
	}

	nodes := resp.CacheClusters[0].CacheNodes
	if len(nodes) == 0 {
		return fmt.Errorf("failed to find active nodes in the cache cluster with id %s",
			c.config.ClusterId)
	}

	//Construct slice with strings of address:port
	endpoints := make([]string, len(nodes))
	for i, node := range resp.CacheClusters[0].CacheNodes {
		endpoints[i] = fmt.Sprintf("%s:%d", *node.Endpoint.Address, *node.Endpoint.Port)
	}

	return c.serverSelector.SetServers(endpoints...)
}

func (c *Client) updateNodesWithRetry(numRetries int) error {
	var err error
	retryWithBackoff := retryDelay
	for i := 0; i < numRetries; i++ {
		err = c.updateNodes()
		if err == nil {
			return nil
		}
		logger.WithError(err).Warnf("failed to update nodes, retrying in %vs", retryWithBackoff)
		time.Sleep(retryWithBackoff)
		retryWithBackoff *= 2
	}
	return err
}

// StartAutoDiscovery is a blocking function that refreshes nodes on an interval.
func (c *Client) StartAutoDiscovery() {
	tick := time.NewTicker(tickTime)
	for {
		select {
		case <-tick.C:
			err := c.updateNodes()
			if err != nil {
				logger.WithError(err).Error("failed to update nodes")
				continue
			}
		case <-c.closer:
			return
		}
	}
}

// Close is a blocking function that waits to cleanly shut down auto-discovery.
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
		Expiration: int32(c.config.TTL),
	})
}
