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

// cancellation function ensures goroutine has exited before returning
func (d *dispatcher) Run() func() {
	slots := make(chan struct{}, d.NumWorkers)
	for i := 0; i < d.NumWorkers; i++ {
		slots <- struct{}{}
	}

	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				stop = nil
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
						d.Queue.Finish(job)
					}
					slots <- struct{}{}
				}()
			}
		}
	}()

	return func() {
		close(stop)
	}
}
