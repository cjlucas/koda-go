package koda

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestGetQueue(t *testing.T) {
	c := newTestClient()
	if q := c.GetQueue("q"); q == nil {
		t.Error("Queue was nil")
	}
}

func TestWork(t *testing.T) {
	client := newTestClient()
	q := client.GetQueue("q")

	q.Submit(100, nil)

	next := make(chan struct{})
	client.Register("q", 1, func(job Job) error {
		next <- struct{}{}
		return nil
	})

	stop := client.Work()

	select {
	case <-next:
	case <-time.After(1 * time.Second):
		t.Fatal("worker was not called")
		return
	}

	stop()
}

func TestWorkForever(t *testing.T) {
	client := newTestClient()

	next := make(chan struct{})

	go func() {
		client.WorkForever()
		next <- struct{}{}
	}()

	go func() {
		time.Sleep(1 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()

	select {
	case <-next:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out")
	}
}
