package koda

import "github.com/cjlucas/koda-go/internal/mock"

func newTestClient() *Client {
	client := mock.NewConn()
	return NewClient(&Options{
		ConnFactory: func() Conn {
			return client
		},
	})
}

func newQueue(name string) Queue {
	return Queue{
		Name:        name,
		NumWorkers:  1,
		MaxAttempts: 1,
	}
}
