package koda

import (
	"net"
	"strconv"
	"time"
)

const minPriority = 0
const maxPriority = 100

type Queue struct {
	name   string
	client *Client
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
	return q.client.buildKey("queue", q.name, strconv.Itoa(priority))
}

func (q *Queue) delayedKey() string {
	return q.client.buildKey("delayed_queue", q.name)
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
	_, err := conn.LPush(q.key(j.Priority), q.jobKey(j.ID))
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

// TODO: Move to Client
func (q *Queue) Retry(j *Job, d time.Duration) error {
	conn := q.client.getConn()
	defer q.client.putConn(conn)

	j.DelayedUntil = time.Now().UTC().Add(d)

	if err := q.persistJob(j, conn, "delayed_until"); err != nil {
		return err
	}

	return q.addJobToDelayedQueue(j, conn)
}

// TODO: Move to Client
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

func (q *Queue) Wait() (*Job, error) {
	conn := q.client.getConn()
	defer q.client.putConn(conn)

	queues := make([]string, maxPriority-minPriority+1)
	for i := minPriority; i <= maxPriority; i++ {
		queues[i] = q.key(i)
	}

	delayedQueueKey := q.delayedKey()

	var jobKey string
	for {
		results, err := conn.BRPop(1*time.Second, queues...)
		if err != nil && err != NilError {
			if err, ok := err.(net.Error); ok && err.Temporary() {
				// TODO(clucas): In backoff algorithm may be appropriate here
				time.Sleep(5 * time.Second)
				continue
			}

			return nil, err
		}

		if len(results) > 1 {
			jobKey = results[1]
			break
		}

		results, err = conn.ZRangeByScore(delayedQueueKey, ZRangeByScoreOpts{
			Min:          0,
			Max:          timeAsFloat(time.Now().UTC()),
			MinInclusive: true,
			MaxInclusive: true,
			Offset:       0,
			Count:        1,
		})

		if err != nil {
			return nil, err
		}

		if len(results) > 0 {
			jobKey = results[0]
			numRemoved, err := conn.ZRem(delayedQueueKey, jobKey)
			if err != nil {
				return nil, err
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
				continue
			} else {
				break
			}
		}

	}

	j, err := unmarshalJob(conn, jobKey)
	if err != nil {
		return nil, err
	}

	j.State = Working
	j.NumAttempts++
	j.Queue = q
	j.Client = q.client

	return j, q.persistJob(j, conn, "state", "num_attempts")
}
