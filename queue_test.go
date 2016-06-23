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
		{Priority: 100, Payload: map[string]string{"foo": "bar"}},
	}

	curID := 0
	for _, c := range cases {
		job, err := client.GetQueue("q").Submit(c.Priority, c.Payload)
		if err != nil {
			t.Fatal(err)
		}
		curID++

		// TODO: compare against job returned by Queue.Job as well

		if job.ID != curID {
			t.Errorf("unexpected id: %d != %d", job.ID, curID)
		}

		if !reflect.DeepEqual(c.Payload, job.Payload) {
			t.Errorf("payload mismatch: %s != %s", c.Payload, job.Payload)
		}

		if c.Priority != job.Priority {
			t.Errorf("priority mismatch: %d != %d", c.Priority, job.Priority)
		}
	}
}
