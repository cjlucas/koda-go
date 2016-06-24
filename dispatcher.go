package koda

import "time"

type HandlerFunc func(c *Client, j Job)

type dispatcher struct {
	Queue      *Queue
	NumWorkers int
	slots      chan struct{}
	Handler    HandlerFunc
}

func (d *dispatcher) Run() {
	slots := make(chan struct{}, d.NumWorkers)
	for i := 0; i < d.NumWorkers; i++ {
		slots <- struct{}{}
	}

	for {
		<-slots

		j, err := d.Queue.Wait()
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}

		go func() {
			d.Handler(d.Queue.client, *j)
			slots <- struct{}{}
		}()
	}
}
