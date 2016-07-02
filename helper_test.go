package koda

import "github.com/cjlucas/koda-go/internal/mock"

func optionsWithMock() *Options {
	client := mock.NewConn()
	return &Options{
		ConnFactory: func() Conn {
			return client
		},
	}
}

func newTestClient() *Client {
	return NewClient(optionsWithMock())
}

func newQueue(name string) Queue {
	return Queue{
		Name:        name,
		NumWorkers:  1,
		MaxAttempts: 1,
	}
}
