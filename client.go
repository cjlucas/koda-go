package koda

import (
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"
)

var DefaultClient = NewClient(nil)

type Client struct {
	opts        *Options
	connPool    sync.Pool
	dispatchers map[string]*dispatcher
}

type Options struct {
	URL         string
	Prefix      string
	ConnFactory func() Conn
}

func (c *Client) newQueue(name string) *Queue {
	q := &Queue{
		Name:   name,
		client: c,
	}

	q.queueKeys = make([]string, maxPriority-minPriority+1)
	i := 0
	for j := maxPriority; j >= minPriority; j-- {
		q.queueKeys[i] = c.priorityQueueKey(name, j)
		i++
	}

	return q
}

func (c *Client) Job(id int) (Job, error) {
	conn := c.getConn()
	defer c.putConn(conn)

	job, err := unmarshalJob(conn, c.jobKey(id))
	return *job, err
}

func (c *Client) Queue(name string) *Queue {
	return c.newQueue(name)
}

func (c *Client) persistJob(j *Job, conn Conn, fields ...string) error {
	jobKey := c.jobKey(j.ID)
	hash, err := j.hash()
	if err != nil {
		return err
	}

	if len(fields) == 0 {
		for k := range hash {
			fields = append(fields, k)
		}
	}

	out := make(map[string]string)
	for _, f := range fields {
		out[f] = hash[f]
	}

	return conn.HSetAll(jobKey, out)
}

func (c *Client) addJobToQueue(queueName string, j *Job, conn Conn) error {
	_, err := conn.RPush(c.priorityQueueKey(queueName, j.Priority), c.jobKey(j.ID))
	return err
}

func (c *Client) Submit(queue Queue, priority int, payload interface{}) (*Job, error) {
	conn := c.getConn()
	defer c.putConn(conn)

	j := &Job{
		Payload:  payload,
		Priority: priority,
		State:    Queued,
	}

	if err := c.persistNewJob(j, conn); err != nil {
		return nil, err
	}

	return j, c.addJobToQueue(queue.Name, j, conn)
}

func (c *Client) addJobToDelayedQueue(queueName string, j *Job, conn Conn) error {
	_, err := conn.ZAddNX(c.delayedQueueKey(queueName), timeAsFloat(j.DelayedUntil), c.jobKey(j.ID))
	return err
}

func (c *Client) SubmitDelayed(queue Queue, d time.Duration, payload interface{}) (*Job, error) {
	conn := c.getConn()
	defer c.putConn(conn)

	j := &Job{
		Payload:      payload,
		DelayedUntil: time.Now().Add(d).UTC(),
		State:        Queued,
	}

	if err := c.persistNewJob(j, conn); err != nil {
		return nil, err
	}

	return j, c.addJobToDelayedQueue(queue.Name, j, conn)
}

func (c *Client) Register(queue string, numWorkers int, f HandlerFunc) {
	c.dispatchers[queue] = &dispatcher{
		Queue:         c.Queue(queue),
		NumWorkers:    numWorkers,
		Handler:       f,
		MaxRetries:    5,
		RetryInterval: 5 * time.Second,
	}
}

// Canceller allows the user to cancel all working jobs. If timeout is not set,
// all currently working jobs will immediately be marked failed.
type Canceller interface {
	Cancel()
	CancelWithTimeout(d time.Duration)
}

type canceller struct {
	dispatchers []*dispatcher
}

func (c *canceller) Cancel() {
	c.CancelWithTimeout(0)
}

func (c *canceller) CancelWithTimeout(d time.Duration) {
	n := len(c.dispatchers)
	if n == 0 {
		return
	}

	done := make(chan struct{}, n)
	for i := range c.dispatchers {
		di := c.dispatchers[i]
		go func() {
			di.Cancel(d)
			done <- struct{}{}
		}()
	}

	for i := 0; i < n; i++ {
		<-done
	}
}

func (c *Client) Work() Canceller {
	var dispatchers []*dispatcher
	for _, d := range c.dispatchers {
		d.Run()
		dispatchers = append(dispatchers, d)
	}

	return &canceller{dispatchers: dispatchers}
}

func (c *Client) WorkForever() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	canceller := c.Work()

	<-sig
	signal.Stop(sig)
	canceller.CancelWithTimeout(0)
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

func timeAsFloat(t time.Time) float64 {
	// time.Second is the number of nanoseconds in one second
	// return float64(t.Unix())
	return float64(t.UTC().UnixNano()) / float64(time.Second)
}

func (c *Client) persistNewJob(j *Job, conn Conn) error {
	id, err := c.incrJobID(conn)
	if err != nil {
		return err
	}

	j.ID = id
	j.CreationTime = time.Now().UTC()

	return c.persistJob(j, conn)
}

func (c *Client) priorityQueueKey(queueName string, priority int) string {
	return c.buildKey("queue", queueName, strconv.Itoa(priority))
}

func (c *Client) delayedQueueKey(queueName string) string {
	return c.buildKey("delayed_queue", queueName)
}

func (c *Client) jobKey(id int) string {
	return c.buildKey("jobs", strconv.Itoa(id))
}

func (c *Client) incrJobID(conn Conn) (int, error) {
	return conn.Incr(c.buildKey("cur_job_id"))
}
