package koda

import "time"

type HandlerFunc func(c *Client, j Job)

type dispatcher struct {
	Queue      *Queue
	NumWorkers int
	Handler    HandlerFunc
}

func (d *dispatcher) Run() chan<- struct{} {
	slots := make(chan struct{}, d.NumWorkers)
	for i := 0; i < d.NumWorkers; i++ {
		slots <- struct{}{}
	}

	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				return
			case <-slots:
				j, err := d.Queue.Wait()
				if err != nil {
					time.Sleep(5 * time.Second)
					continue
				}

				d.Queue.updateState(*j, Working)

				go func() {
					d.Handler(d.Queue.client, *j)
					slots <- struct{}{}
				}()
			}
		}
	}()

	return stop
}
