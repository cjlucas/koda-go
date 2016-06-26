package koda

import (
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/redis.v3"
)

var DefaultClient = NewClient(nil)

type Client struct {
	opts        *Options
	connPool    sync.Pool
	dispatchers map[string]dispatcher
}

type Options struct {
	URL         string
	Prefix      string
	ConnFactory func() Conn
}

func Configure(opts *Options) {
	DefaultClient = NewClient(opts)
}

func NewClient(opts *Options) *Client {
	if opts == nil {
		opts = &Options{}
	}

	if opts.URL == "" {
		opts.URL = "redis://localhost:6379"
	}

	if opts.Prefix == "" {
		opts.Prefix = "koda"
	}

	if opts.ConnFactory == nil {
		url, err := url.Parse(opts.URL)
		db, _ := strconv.Atoi(url.Path)

		// TODO: return err
		if err != nil {
			panic(err)
		}

		opts.ConnFactory = func() Conn {
			r := redis.NewClient(&redis.Options{
				Addr: url.Host,
				DB:   int64(db),
			})
			return &GoRedisAdapter{R: r}
		}
	}

	return &Client{
		opts:        opts,
		dispatchers: make(map[string]dispatcher),
		connPool: sync.Pool{New: func() interface{} {
			return opts.ConnFactory()
		}},
	}
}

// TODO: Rename to Queue
func (c *Client) GetQueue(name string) *Queue {
	return &Queue{
		name:   name,
		client: c,
	}
}

func (c *Client) Register(queue string, numWorkers int, f HandlerFunc) {
	c.dispatchers[queue] = dispatcher{
		Queue:         c.GetQueue(queue),
		NumWorkers:    numWorkers,
		Handler:       f,
		MaxRetries:    5,
		RetryInterval: 5 * time.Second,
	}
}

func (c *Client) Work() chan struct{} {
	ch := make(chan struct{})

	var stoppers []chan<- struct{}
	for _, d := range c.dispatchers {
		stoppers = append(stoppers, d.Run())
	}

	go func() {
		<-ch
		close(ch)
		for _, c := range stoppers {
			c <- struct{}{}
		}
	}()

	return ch
}

func (c *Client) getConn() Conn {
	return c.connPool.Get().(Conn)
}

func (c *Client) putConn(conn Conn) {
	c.connPool.Put(conn)
}

func (c *Client) buildKey(s ...string) string {
	s = append([]string{c.opts.Prefix}, s...)
	return strings.Join(s, ":")
}
