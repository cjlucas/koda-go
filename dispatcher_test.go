package koda

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestDispatcherRun(t *testing.T) {
	// submit N+1 jobs to a queue
	N := 5
	c := newTestClient()
	jobIDs := make([]int, N+1)

	q := c.GetQueue("q")
	for i := 0; i < N+1; i++ {
		job, err := q.Submit(100, nil)
		if err != nil {
			t.Fatal("failed to add job")
			return
		}
		jobIDs = append(jobIDs, job.ID)
	}

	// register N workers to work that queue
	lock := sync.Mutex{}
	dispatcher := dispatcher{
		Queue:      q,
		NumWorkers: N,
		Handler: func(c *Client, job Job) {
			// TODO: you also want to assert this function was actually called N times
			lock.Lock()
			lock.Unlock()
		},
	}

	lock.Lock()
	stop := dispatcher.Run()

	// TODO: Sleeps are a hack. The real way to do it would be to
	// expose the mockConn's lock (maybe...)
	time.Sleep(1 * time.Second)
	stop <- struct{}{}
	time.Sleep(1 * time.Second)
	lock.Unlock()

	for i := 0; i < N; i++ {
		job, err := q.Job(jobIDs[i])
		if err != nil {
			t.Fatal("could not get job")
			return
		}

		if job.State != Working {
			t.Errorf("job is not working: %s", job.State)
		}
	}

	fmt.Printf("%#v\n", c)

	job, err := q.Job(jobIDs[N-1])
	if err != nil {
		t.Fatal("could not get job")
		return
	}

	if job.State != Queued {
		t.Error("last job is not queued")
	}

	// the worker shall be a simple lock/unlock (or chan equiv.) where
	// lock is defined in this scope. When Run() is called in a seperate
	// goroutine (logistics...! Run now returns a chan to stop it) check the mock redis conn for how many
	// jobs were processed. If the answer is > N-1, the test should fail.
	// This scope should then unlock and exit the function
}
