package koda

import "testing"

func TestGetQueue(t *testing.T) {
	c := newTestClient()
	if q := c.GetQueue("q"); q == nil {
		t.Error("Queue was nil")
	}
}
