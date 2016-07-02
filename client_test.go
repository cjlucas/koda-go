package koda

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestCreateJob(t *testing.T) {
	client := newTestClient()

	job, err := client.CreateJob(map[string]string{
		"data": "stuff",
	})

	if err != nil {
		t.Fatal(err)
	}

	job, err = client.Job(job.ID)
	if err != nil {
		t.Fatal(err)
	}

	if job.State != Initial {
		t.Error("unexpected job state:", job.State)
	}
}

func testJobSubmission(t *testing.T, submitFunc func(client *Client, job Job) (Job, error)) {
	client := newTestClient()

	job, err := client.CreateJob(nil)
	if err != nil {
		t.Fatal(err)
	}

	job, err = submitFunc(client, job)
	if err != nil {
		t.Fatal(err)
	}

	if job.State != Queued {
		t.Error("unexpected job state:", job.State)
	}
}

func TestSubmitJob(t *testing.T) {
	testJobSubmission(t, func(client *Client, job Job) (Job, error) {
		return client.SubmitJob(Queue{Name: "q"}, 100, job)
	})
}

func TestSubmitDelayedJob(t *testing.T) {
	testJobSubmission(t, func(client *Client, job Job) (Job, error) {
		return client.SubmitDelayedJob(Queue{Name: "q"}, 0, job)
	})
}

func TestWork(t *testing.T) {
	client := newTestClient()
	q := Queue{Name: "q"}

	client.Submit(q, 100, nil)

	next := make(chan struct{})
	client.Register(q, func(job *Job) error {
		next <- struct{}{}
		return nil
	})

	canceller := client.Work()

	select {
	case <-next:
	case <-time.After(1 * time.Second):
		t.Fatal("worker was not called")
		return
	}

	canceller.Cancel()
}

func TestWorkForever(t *testing.T) {
	client := newTestClient()
	q := Queue{
		Name:        "q",
		MaxAttempts: 2,
	}

	next := make(chan struct{})
	client.Register(q, func(job *Job) error {
		next <- struct{}{}
		return nil
	})

	job, _ := client.Submit(q, 100, nil)

	done := make(chan struct{})
	go func() {
		client.WorkForever()
		done <- struct{}{}
	}()

	go func() {
		time.Sleep(1 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out")
	}

	// Allow worker to complete
	<-next

	j, _ := client.Job(job.ID)
	if j.State != Queued || j.NumAttempts != 1 {
		t.Fatal("job should be readded to queue")
	}
}
