package koda

import (
	"net/url"
	"strconv"
	"sync"
	"time"

	"gopkg.in/redis.v3"
)

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
		opts: opts,
		connPool: sync.Pool{New: func() interface{} {
			return opts.ConnFactory()
		}},
	}
}

func Configure(opts *Options) {
	DefaultClient = NewClient(opts)
}

func Submit(queue string, priority int, payload interface{}) (*Job, error) {
	return DefaultClient.Submit(Queue{Name: queue}, priority, payload)
}

func SubmitDelayed(queue string, d time.Duration, payload interface{}) (*Job, error) {
	return DefaultClient.SubmitDelayed(Queue{Name: queue}, d, payload)
}

func Register(queue string, numWorkers int, f HandlerFunc) {
	q := Queue{
		Name:       queue,
		NumWorkers: numWorkers,
	}
	DefaultClient.Register(q, f)
}

func Work() Canceller {
	return DefaultClient.Work()
}

func WorkForever() {
	DefaultClient.WorkForever()
}
