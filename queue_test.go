package koda

import (
	"reflect"
	"testing"
	"time"
)

func TestSubmit(t *testing.T) {
	client := newTestClient()

	cases := []struct {
		Priority int
		Payload  interface{}
	}{
		{Priority: 100, Payload: nil},
		{Priority: 50, Payload: map[string]interface{}{"foo": "bar"}},
	}

	curID := 0
	q := client.GetQueue("q")
	for _, c := range cases {
		job, err := q.Submit(c.Priority, c.Payload)
		if err != nil {
			t.Fatal(err)
		}
		curID++

		equals := func(job Job) {
			if job.ID != curID {
				t.Errorf("unexpected id: %d != %d", job.ID, curID)
			}

			if job.CreationTime.IsZero() {
				t.Error("creation time was not set")
			}

			if job.State != Queued {
				t.Errorf("unexpected job state: %s", job.State)
			}

			if !reflect.DeepEqual(c.Payload, job.Payload) {
				t.Errorf("payload mismatch: %#v != %#v", c.Payload, job.Payload)
			}

			if c.Priority != job.Priority {
				t.Errorf("priority mismatch: %d != %d", c.Priority, job.Priority)
			}
		}

		j, err := q.Job(job.ID)
		if err != nil {
			t.Fatal(err)
		}

		equals(*job)
		equals(j)
	}
}

func TestWait(t *testing.T) {
	client := newTestClient()
	q := client.GetQueue("q")
	q.Submit(100, nil)

	job, err := q.Wait()
	if err != nil {
		t.Fatalf("error while waiting: %s", err)
	}

	if job.State != Working {
		t.Errorf("incorrect state: %s", job.State)
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
	defer func() { stop <- struct{}{} }()

	select {
	case <-next:
		break
	case <-time.After(1 * time.Second):
		t.Fatal("worker was not called")
		return
	}
}
