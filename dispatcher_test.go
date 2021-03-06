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

	q := Queue{Name: "q", NumWorkers: N}
	for i := 0; i < N+1; i++ {
		job, err := c.Submit(q, 100, nil)
		if err != nil {
			t.Fatal("failed to add job")
			return
		}
		jobIDs[i] = job.ID
	}

	// register N workers to work that queue
	next := make(chan struct{})
	defer close(next)
	hits := make(chan struct{}, N+1)
	defer close(hits)

	lock := sync.Mutex{}
	dispatcher := dispatcher{
		Queue:  q,
		client: c,
		Handler: func(job *Job) error {
			hits <- struct{}{}
			if len(hits) >= N {
				next <- struct{}{}
			}

			lock.Lock()
			lock.Unlock()
			return nil
		},
	}

	lock.Lock()
	dispatcher.Run()
	defer dispatcher.Cancel(0)

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
		job, err := c.Job(id)
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

	// drain last job from dispatcher
	<-next
}

func TestDispatcherRun_Retry(t *testing.T) {
	n := 5
	c := newTestClient()
	q := Queue{
		Name:        "q",
		NumWorkers:  1,
		MaxAttempts: n,
	}

	job, _ := c.Submit(q, 100, nil)

	hits := 0
	next := make(chan struct{})
	dispatcher := dispatcher{
		Queue:  q,
		client: c,
		Handler: func(job *Job) error {
			hits++
			if hits == n {
				next <- struct{}{}
			}
			return errors.New("")
		},
	}
	dispatcher.Run()

	select {
	case <-next:
		break
	case <-time.After(1 * time.Second):
		t.Fatal("timed out")
		return
	}

	dispatcher.Cancel(0)

	j, _ := c.Job(job.ID)
	if j.State != Dead {
		t.Error("incorrect state:", j.State)
	}
}

func TestDispatcher(t *testing.T) {
	c := newTestClient()
	q := newQueue("q")

	job, _ := c.Submit(q, 100, nil)

	next := make(chan struct{})
	lock := sync.Mutex{}
	dispatcher := dispatcher{
		Queue:  q,
		client: c,
		Handler: func(job *Job) error {
			next <- struct{}{}
			lock.Lock()
			lock.Unlock()
			return nil
		},
	}

	lock.Lock()
	defer lock.Unlock()

	dispatcher.Run()
	<-next

	dispatcher.Cancel(0)

	j, _ := c.Job(job.ID)

	if j.State != Dead {
		t.Error("job was not marked as dead: ", job.State)
	}
}

func TestDispatcherCancel_Timeout(t *testing.T) {
	c := newTestClient()
	q := newQueue("q")

	job, _ := c.Submit(q, 100, nil)

	next := make(chan struct{})
	dispatcher := dispatcher{
		Queue:  q,
		client: c,
		Handler: func(job *Job) error {
			next <- struct{}{}
			next <- struct{}{}
			return nil
		},
	}

	dispatcher.Run()
	<-next

	done := make(chan struct{})
	go func() {
		<-done
		dispatcher.Cancel(1 * time.Second)
		done <- struct{}{}
	}()
	done <- struct{}{}
	<-next

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out")
	}

	j, _ := c.Job(job.ID)

	if j.State != Finished {
		t.Error("job was not marked as finished: ", j.State)
	}
}
