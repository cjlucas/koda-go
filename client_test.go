package koda

import (
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestWork(t *testing.T) {
	client := newTestClient()
	q := Queue{Name: "q"}

	client.Submit(q, 100, nil)

	next := make(chan struct{})
	client.Register(q, func(job Job) error {
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
	client.Register(q, func(job Job) error {
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
		fmt.Println(j)
		t.Fatal("job should be readded to queue")
	}
}
