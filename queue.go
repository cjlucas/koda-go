package koda

import (
	"errors"
	"fmt"
	"time"
)

const minPriority = 0
const maxPriority = 100

type Queue struct {
	Name      string
	client    *Client
	queueKeys []string
}

func (q *Queue) Retry(j *Job, d time.Duration) error {
	conn := q.client.getConn()
	defer q.client.putConn(conn)

	j.State = Queued
	j.DelayedUntil = time.Now().UTC().Add(d)

	if err := q.client.persistJob(j, conn, "state", "delayed_until"); err != nil {
		return err
	}

	return q.client.addJobToDelayedQueue(q.Name, j, conn)
}

func (q *Queue) Finish(j Job) error {
	conn := q.client.getConn()
	defer q.client.putConn(conn)

	j.State = Finished
	j.CompletionTime = time.Now().UTC()

	return q.client.persistJob(&j, conn, "state", "completion_time")
}

func (q *Queue) Kill(j *Job) error {
	conn := q.client.getConn()
	defer q.client.putConn(conn)

	j.State = Dead
	return q.client.persistJob(j, conn, "state")
}

func (q *Queue) Job(id int) (Job, error) {
	c := q.client.getConn()
	defer q.client.putConn(c)

	job, err := unmarshalJob(c, q.client.jobKey(id))
	return *job, err
}

func (q *Queue) wait(conn Conn, queues ...string) (string, error) {
	delayedQueueKey := q.client.delayedQueueKey(q.Name)
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

	results, err = conn.BLPop(1*time.Second, queues...)
	if err != nil && err != NilError {
		return "", err
	}

	if len(results) > 1 {
		return results[1], nil
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

	q.client.persistJob(j, conn, "state", "num_attempts")

	return *j, nil
}
