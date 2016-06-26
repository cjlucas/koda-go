package koda

import (
	"errors"
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
		jobIDs[i] = job.ID
	}

	// register N workers to work that queue
	next := make(chan struct{})
	lock := sync.Mutex{}
	hits := make(chan struct{}, N+1)
	defer close(hits)

	dispatcher := dispatcher{
		Queue:      q,
		NumWorkers: N,
		Handler: func(job Job) error {
			hits <- struct{}{}
			if len(hits) == N {
				next <- struct{}{}
			}

			lock.Lock()
			lock.Unlock()
			return nil
		},
	}

	lock.Lock()
	stop := dispatcher.Run()
	defer stop()

	select {
	case <-next:
		break
	case <-time.After(1 * time.Second):
		t.Fatal("timed out")
		return
	}

	// snapshot jobs
	jobs := make([]Job, len(jobIDs))
	for i, id := range jobIDs {
		job, err := q.Job(id)
		if err != nil {
			t.Fatal("could not get job")
			return
		}
		jobs[i] = job
	}

	if len(hits) != N {
		t.Errorf("handler was hit only %d times", len(hits))
	}

	lock.Unlock()
	for i := 0; i < N; i++ {
		if jobs[i].State != Working {
			t.Errorf("job is not working: %s", jobs[i].State)
		}
	}

	if jobs[N].State != Queued {
		t.Error("last job is not queued:", jobs[N].State)
	}
}

func TestDispatcherRun_Retry(t *testing.T) {
	c := newTestClient()
	q := c.GetQueue("q")

	job, _ := q.Submit(100, nil)

	n := 5
	hits := 0
	next := make(chan struct{})
	dispatcher := dispatcher{
		Queue:      q,
		NumWorkers: 1,
		MaxRetries: n,
		Handler: func(job Job) error {
			hits++
			if hits == n {
				next <- struct{}{}
			}
			return errors.New("")
		},
	}
	stop := dispatcher.Run()

	select {
	case <-next:
		break
	case <-time.After(1 * time.Second):
		t.Fatal("timed out")
		return
	}

	stop()

	j, _ := q.Job(job.ID)
	if j.State != Dead {
		t.Error("incorrect state:", j.State)
	}
}
