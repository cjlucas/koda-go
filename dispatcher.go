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

func (d *dispatcher) Run() chan struct{} {
	slots := make(chan struct{}, d.NumWorkers)
	for i := 0; i < d.NumWorkers; i++ {
		slots <- struct{}{}
	}

	stop := make(chan struct{})

	go func() {
		defer close(stop)

		for {
			select {
			case <-stop:
				return
			case <-slots:
				job, err := d.Queue.Wait()
				if job.ID == 0 || err != nil {
					slots <- struct{}{}
					break
				}

				go func() {
					err := d.Handler(job)
					if err != nil {
						if job.NumAttempts < d.MaxRetries {
							d.Queue.Retry(&job, d.RetryInterval)
						} else {
							d.Queue.Kill(&job)
						}
					} else {
						job.Finish()
					}
					slots <- struct{}{}
				}()
			}
		}
	}()

	return stop
}
