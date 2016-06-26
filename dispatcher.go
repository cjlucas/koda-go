package koda

import "time"

type HandlerFunc func(j Job) error

type dispatcher struct {
	Queue         *Queue
	NumWorkers    int
	Handler       HandlerFunc
	MaxRetries    int
	RetryInterval time.Duration
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
						if j.NumAttempts < d.MaxRetries {
							d.Queue.Retry(j, d.RetryInterval)
						} else {
							d.Queue.Kill(j)
						}
					} else {
						j.Finish()
					}
					slots <- struct{}{}
				}()
			}
		}
	}()

	return stop
}
