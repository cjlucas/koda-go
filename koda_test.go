package koda

import "testing"

func TestConfigure(t *testing.T) {
	oldClient := DefaultClient
	Configure(optionsWithMock())
	if DefaultClient == oldClient {
		t.Error("DefaultClient did not change")
	}
}
