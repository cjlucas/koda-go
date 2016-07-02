package koda

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"
)

// HandlerFunc is used when registering a worker for a queue. If an error
// is returned, the job will be marked as failed. If Job.NumAttempts exceeds
// Queue.MaxAttempts, the job is placed in the Dead state. Otherwise it
// is placed on the delayed queue with a delay of Queue.RetryInterval
type HandlerFunc func(j Job) error

// DefaultClient is the Client used by the package-level functions.
var DefaultClient = NewClient(nil)

// Client represents a managed koda client. Users should not attempt
// instantiate their own Client, but instead should use NewClient.
type Client struct {
	opts        *Options
	connPool    sync.Pool
	dispatchers []*dispatcher
}

// Options for a Client.
type Options struct {
	// Redis URL (format: redis://[auth@]host[:port][/database])
	// Default: redis://localhost:6379
	URL string

	// Prefix for redis keys
	// Default: koda
	Prefix string

	ConnFactory func() Conn
}

// CreateJob will create a job in the Initial state.
func (c *Client) CreateJob(payload interface{}) (Job, error) {
	conn := c.getConn()
	defer c.putConn(conn)

	job := Job{payload: payload}
	err := c.persistNewJob(&job, conn)
	return job, err
}

// Job fetches a job with the given job ID
func (c *Client) Job(id int) (Job, error) {
	conn := c.getConn()
	defer c.putConn(conn)

	job, err := unmarshalJob(conn, c.jobKey(id))
	return *job, err
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

// Submit creates a job and puts it on the priority queue.
func (c *Client) Submit(queue Queue, priority int, payload interface{}) (*Job, error) {
	conn := c.getConn()
	defer c.putConn(conn)

	j := &Job{
		payload:  payload,
		Priority: priority,
		State:    Queued,
	}

	if err := c.persistNewJob(j, conn); err != nil {
		return nil, err
	}

	return j, c.addJobToQueue(queue.Name, j, conn)
}

func (c *Client) SubmitJob(queue Queue, priority int, job Job) (Job, error) {
	conn := c.getConn()
	defer c.putConn(conn)

	job, err := c.Job(job.ID)
	if err != nil {
		return Job{}, fmt.Errorf("could not fetch job: %s", err)
	}

	if job.State != Initial {
		return Job{}, fmt.Errorf("invalid job state: %s", job.State)
	}

	job.Priority = priority
	job.State = Queued

	return job, c.addJobToQueue(queue.Name, &job, conn)
}

func (c *Client) addJobToDelayedQueue(queueName string, j *Job, conn Conn) error {
	_, err := conn.ZAddNX(c.delayedQueueKey(queueName), timeAsFloat(j.DelayedUntil), c.jobKey(j.ID))
	return err
}

// SubmitDelayed creates a job and puts it on the delayed queue.
func (c *Client) SubmitDelayed(queue Queue, d time.Duration, payload interface{}) (*Job, error) {
	conn := c.getConn()
	defer c.putConn(conn)

	j := &Job{
		payload:      payload,
		DelayedUntil: time.Now().Add(d).UTC(),
		State:        Queued,
	}

	if err := c.persistNewJob(j, conn); err != nil {
		return nil, err
	}

	return j, c.addJobToDelayedQueue(queue.Name, j, conn)
}

func (c *Client) SubmitDelayedJob(queue Queue, d time.Duration, job Job) (Job, error) {
	conn := c.getConn()
	defer c.putConn(conn)

	job, err := c.Job(job.ID)
	if err != nil {
		return Job{}, fmt.Errorf("could not fetch job: %s", err)
	}

	if job.State != Initial {
		return Job{}, fmt.Errorf("invalid job state: %s", job.State)
	}

	job.DelayedUntil = time.Now().Add(d).UTC()
	job.State = Queued

	return job, c.addJobToDelayedQueue(queue.Name, &job, conn)
}

// Register a HandlerFunc for a given Queue
func (c *Client) Register(queue Queue, f HandlerFunc) {
	if queue.MaxAttempts < 1 {
		queue.MaxAttempts = 1
	}
	if queue.NumWorkers < 1 {
		queue.NumWorkers = 1
	}

	c.dispatchers = append(c.dispatchers, &dispatcher{
		Queue:   queue,
		Handler: f,
	})
}

func (c *Client) retry(j *Job, queue Queue) error {
	conn := c.getConn()
	defer c.putConn(conn)

	j.State = Queued
	j.DelayedUntil = time.Now().UTC().Add(queue.RetryInterval)

	if err := c.persistJob(j, conn, "state", "delayed_until"); err != nil {
		return err
	}

	return c.addJobToDelayedQueue(queue.Name, j, conn)
}

func (c *Client) finish(j *Job) error {
	conn := c.getConn()
	defer c.putConn(conn)

	j.State = Finished
	j.CompletionTime = time.Now().UTC()

	return c.persistJob(j, conn, "state", "completion_time")
}

func (c *Client) kill(j *Job) error {
	conn := c.getConn()
	defer c.putConn(conn)

	j.State = Dead
	return c.persistJob(j, conn, "state")
}

func (c *Client) popJob(conn Conn, delayedQueueKey string, priorityQueues ...string) (string, error) {
	results, err := conn.ZPopByScore(
		delayedQueueKey,
		0,
		timeAsFloat(time.Now().UTC()),
		true,
		true,
		0,
		1)

	if err != nil {
		return "", err
	}

	if len(results) > 0 {
		return results[0], nil
	}

	results, err = conn.BLPop(1*time.Second, priorityQueues...)
	if err != nil {
		return "", err
	}

	if len(results) > 1 {
		return results[1], nil
	}

	return "", nil
}

func (c *Client) wait(queue Queue) (Job, error) {
	conn := c.getConn()
	defer c.putConn(conn)

	if len(queue.queueKeys) == 0 {
		queue.queueKeys = make([]string, maxPriority-minPriority+1)
		i := 0
		for j := maxPriority; j >= minPriority; j-- {
			queue.queueKeys[i] = c.priorityQueueKey(queue.Name, j)
			i++
		}
	}

	jobKey, err := c.popJob(conn, c.delayedQueueKey(queue.Name), queue.queueKeys...)
	if jobKey == "" {
		return Job{}, errors.New("not found")
	}
	if err != nil {
		return Job{}, err
	}

	j, err := unmarshalJob(conn, jobKey)
	if err != nil {
		fmt.Println("error while unmarshaling job", err)
		return Job{}, err
	}

	j.State = Working
	j.NumAttempts++

	c.persistJob(j, conn, "state", "num_attempts")

	return *j, nil
}

// Work will begin processing any registered queues in a seperate goroutine.
// Use returned Canceller to stop any outstanding workers.
func (c *Client) Work() Canceller {
	for _, d := range c.dispatchers {
		d.client = c
		d.Run()
	}

	return &canceller{dispatchers: c.dispatchers}
}

// WorkForever will being processing registered queues. This routine will
// block until SIGINT is received.
func (c *Client) WorkForever() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	canceller := c.Work()

	<-sig
	signal.Stop(sig)
	canceller.CancelWithTimeout(0)
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
