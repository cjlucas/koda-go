package koda

import (
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
		break
	case <-time.After(1 * time.Second):
		t.Fatal("worker was not called")
		return
	}

	stop <- struct{}{}
	<-stop
}
