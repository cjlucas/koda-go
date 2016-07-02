package koda

import (
	"reflect"
	"testing"
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
	q := Queue{Name: "q"}
	for _, c := range cases {
		job, err := client.Submit(q, c.Priority, c.Payload)
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

			if !reflect.DeepEqual(c.Payload, job.payload) {
				t.Errorf("payload mismatch: %#v != %#v", c.Payload, job.payload)
			}

			if c.Priority != job.Priority {
				t.Errorf("priority mismatch: %d != %d", c.Priority, job.Priority)
			}
		}

		j, err := client.Job(job.ID)
		if err != nil {
			t.Fatal(err)
		}

		equals(*job)
		equals(j)
	}
}

func TestWait(t *testing.T) {
	client := newTestClient()
	q := Queue{Name: "q"}
	client.Submit(q, 100, nil)

	job, err := client.wait(q)
	if err != nil {
		t.Fatal(err)
	}

	if job.State != Working {
		t.Errorf("incorrect state: %s", job.State)
	}
}

func TestWait_Delayed(t *testing.T) {
	client := newTestClient()
	q := Queue{Name: "q"}

	job, _ := client.SubmitDelayed(q, 0, nil)

	j, err := client.wait(q)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != j.ID {
		t.Errorf("id mismatch: %d != %d", job.ID, j.ID)
	}
}

func TestWait_Priority(t *testing.T) {
	client := newTestClient()
	q := Queue{Name: "q"}

	job1, _ := client.Submit(q, 100, nil)
	client.Submit(q, 50, nil)

	j, err := client.wait(q)
	if err != nil {
		t.Fatal(err)
	}

	if j.ID != job1.ID {
		t.Fatal("failed to get highest priority job")
	}
}
