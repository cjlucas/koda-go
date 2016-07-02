package koda

import (
	"net/url"
	"strconv"
	"sync"
	"time"

	"gopkg.in/redis.v3"
)

// NewClient creates a new client with the given Options.
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
			return &redisAdapter{R: r}
		}
	}

	return &Client{
		opts: opts,
		connPool: sync.Pool{New: func() interface{} {
			return opts.ConnFactory()
		}},
	}
}

// Configure the DefaultClient with the given Options
func Configure(opts *Options) {
	DefaultClient = NewClient(opts)
}

// Submit creates a job and puts it on the priority queue.
func Submit(queue string, priority int, payload interface{}) (Job, error) {
	return DefaultClient.Submit(Queue{Name: queue}, priority, payload)
}

// SubmitDelayed creates a job and puts it on the delayed queue.
func SubmitDelayed(queue string, d time.Duration, payload interface{}) (Job, error) {
	return DefaultClient.SubmitDelayed(Queue{Name: queue}, d, payload)
}

// Register a given HandlerFunc with a queue
func Register(queue string, numWorkers int, f HandlerFunc) {
	q := Queue{
		Name:       queue,
		NumWorkers: numWorkers,
	}
	DefaultClient.Register(q, f)
}

// Work will begin processing any registered queues in a separate goroutine.
// Use returned Canceller to stop any outstanding workers.
func Work() Canceller {
	return DefaultClient.Work()
}

// WorkForever will being processing registered queues. This routine will
// block until SIGINT is received.
func WorkForever() {
	DefaultClient.WorkForever()
}
