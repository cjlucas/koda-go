package koda

import (
	"sync"
	"testing"
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
	hits := 0
	dispatcher := dispatcher{
		Queue:      q,
		NumWorkers: N,
		Handler: func(job Job) error {
			hits++
			if hits == N {
				next <- struct{}{}
			}
			// TODO: you also want to assert this function was actually called N times
			lock.Lock()
			lock.Unlock()
			return nil
		},
	}

	lock.Lock()
	stop := dispatcher.Run()
	defer func() { stop <- struct{}{} }()

	<-next

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

	if hits != N {
		t.Errorf("handler was hit only %d times", hits)
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
