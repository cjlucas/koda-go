package koda

import "time"

type HandlerFunc func(j Job) error

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

				go func() {
					err := d.Handler(*j)
					if err != nil {
						if j.NumAttempts < 5 {
							d.Queue.Retry(j, 5*time.Minute)
						} else {
							d.Queue.Kill(j)
						}
					}
					slots <- struct{}{}
				}()
			}
		}
	}()

	return stop
}
