package koda

import "testing"

func TestSubmit(t *testing.T) {
	c := newTestClient()
	job, err := c.GetQueue("q").Submit(100, nil)
	if err != nil {
		t.Fatal(err)
	}

	if job.ID != 1 {
		t.Errorf("Unexpected job id %d != 1", job.ID)
	}
}
