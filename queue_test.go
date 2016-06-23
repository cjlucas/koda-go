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
		{Priority: 100, Payload: map[string]interface{}{"foo": "bar"}},
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

			if !reflect.DeepEqual(c.Payload, job.Payload) {
				t.Errorf("payload mismatch: %#v != %#v", c.Payload, job.Payload)
			}

			if c.Priority != job.Priority {
				t.Errorf("priority mismatch: %d != %d", c.Priority, job.Priority)
			}
		}

		// TODO: compare against job returned by Queue.Job as well
		equals(*job)

		j, err := q.Job(job.ID)
		if err != nil {
			t.Fatal(err)
		}

		equals(j)
	}
}
