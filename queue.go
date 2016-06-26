package koda

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

const minPriority = 0
const maxPriority = 100

type Queue struct {
	Name      string
	client    *Client
	queueKeys []string
}

func timeAsFloat(t time.Time) float64 {
	// time.Second is the number of nanoseconds in one second
	// return float64(t.Unix())
	return float64(t.UTC().UnixNano()) / float64(time.Second)
}

func (q *Queue) persistNewJob(j *Job, c Conn) error {
	id, err := q.incrJobID(c)
	if err != nil {
		return err
	}

	j.ID = id
	j.CreationTime = time.Now().UTC()

	return q.persistJob(j, c)
}

func (q *Queue) key(priority int) string {
	return q.client.buildKey("queue", q.Name, strconv.Itoa(priority))
}

func (q *Queue) delayedKey() string {
	return q.client.buildKey("delayed_queue", q.Name)
}

func (q *Queue) jobKey(id int) string {
	return q.client.buildKey("jobs", strconv.Itoa(id))
}

func (q *Queue) incrJobID(c Conn) (int, error) {
	return c.Incr(q.client.buildKey("cur_job_id"))
}

func (q *Queue) persistJob(j *Job, c Conn, fields ...string) error {
	jobKey := q.jobKey(j.ID)
	hash := j.asHash()

	if len(fields) == 0 {
		for k := range hash {
			fields = append(fields, k)
		}
	}

	out := make(map[string]string)
	for _, f := range fields {
		out[f] = hash[f]
	}

	return c.HSetAll(jobKey, out)
}

func (q *Queue) addJobToQueue(j *Job, conn Conn) error {
	_, err := conn.RPush(q.key(j.Priority), q.jobKey(j.ID))
	return err
}

func (q *Queue) Submit(priority int, payload interface{}) (*Job, error) {
	conn := q.client.getConn()
	defer q.client.putConn(conn)

	j := &Job{
		Payload:  payload,
		Priority: priority,
		State:    Queued,
	}

	if err := q.persistNewJob(j, conn); err != nil {
		return nil, err
	}

	return j, q.addJobToQueue(j, conn)
}

func (q *Queue) addJobToDelayedQueue(j *Job, conn Conn) error {
	_, err := conn.ZAddNX(q.delayedKey(), timeAsFloat(j.DelayedUntil), q.jobKey(j.ID))
	return err
}

func (q *Queue) SubmitDelayed(d time.Duration, payload interface{}) (*Job, error) {
	conn := q.client.getConn()
	defer q.client.putConn(conn)

	j := &Job{
		Payload:      payload,
		DelayedUntil: time.Now().Add(d).UTC(),
		State:        Queued,
	}

	if err := q.persistNewJob(j, conn); err != nil {
		return nil, err
	}

	return j, q.addJobToDelayedQueue(j, conn)
}

func (q *Queue) Retry(j *Job, d time.Duration) error {
	conn := q.client.getConn()
	defer q.client.putConn(conn)

	j.DelayedUntil = time.Now().UTC().Add(d)

	if err := q.persistJob(j, conn, "delayed_until"); err != nil {
		return err
	}

	return q.addJobToDelayedQueue(j, conn)
}

func (q *Queue) Finish(j Job) error {
	conn := q.client.getConn()
	defer q.client.putConn(conn)

	j.CompletionTime = time.Now().UTC()

	return q.persistJob(&j, conn, "completion_time")
}

func (q *Queue) Kill(j *Job) error {
	conn := q.client.getConn()
	defer q.client.putConn(conn)

	j.State = Dead

	return q.persistJob(j, conn, "state")
}

func (q *Queue) Job(id int) (Job, error) {
	c := q.client.getConn()
	defer q.client.putConn(c)

	job, err := unmarshalJob(c, q.jobKey(id))
	return *job, err
}

func (q *Queue) wait(conn Conn, queues ...string) (string, error) {
	results, err := conn.BLPop(1*time.Second, queues...)
	if err != nil && err != NilError {
		return "", err
	}

	if len(results) > 1 {
		return results[1], nil
	}

	delayedQueueKey := q.delayedKey()
	results, err = conn.ZRangeByScore(
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
		jobKey := results[0]
		numRemoved, err := conn.ZRem(delayedQueueKey, jobKey)
		if err != nil {
			return "", err
		}

		// NOTE: To prevent a race condition in which multiple clients
		// would get the same job key via ZRangeByScore, as the clients
		// race to remove the job key, the "winner" is the one to successfully
		// remove the key, all other clients should continue waiting for a job
		//
		// Although this solution is logically correct, it could cause
		// thrashing if meeting the race condition is a common occurance.
		// So, an alternate solution may be necessary. Of which a Lua
		// script that performs the zrangebyscore and zrem atomically
		if numRemoved == 0 {
			return "", errors.New("ZRem removed zero keys")
		}

		return jobKey, nil
	}

	return "", nil
}

func (q *Queue) Wait() (Job, error) {
	conn := q.client.getConn()
	defer q.client.putConn(conn)

	jobKey, err := q.wait(conn, q.queueKeys...)
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

	q.persistJob(j, conn, "state", "num_attempts")

	return *j, nil
}
