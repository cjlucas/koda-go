package koda

import (
	"sync"
	"time"
)

type HandlerFunc func(j Job) error

type dispatcher struct {
	Queue      *Queue
	NumWorkers int
	Handler    HandlerFunc
	MaxRetries int

	cancel chan struct{}
	done   chan struct{}
	slots  chan struct{}

	jobs     map[int]Job
	jobsLock sync.Mutex

	RetryInterval time.Duration
}

// Cancel all running jobs. If timeout is set, will block until
// all outstanding workers have returned. If timeout expires, all pending jobs
// are marked as failed
func (d *dispatcher) Cancel(timeout time.Duration) {
	if timeout > 0 {
		done := make(chan struct{})
		go func() {
			for i := 0; i < d.NumWorkers; i++ {
				<-d.slots
			}
			done <- struct{}{}
		}()

		select {
		case <-done:
		case <-time.After(timeout):
		}
	}

	close(d.cancel)
	<-d.done

	d.jobsLock.Lock()
	for _, job := range d.jobs {
		d.jobFailed(job)
		delete(d.jobs, job.ID)
	}
	d.jobsLock.Unlock()
}

func (d *dispatcher) jobFailed(job Job) {
	if job.NumAttempts < d.MaxRetries {
		d.Queue.Retry(&job, d.RetryInterval)
	} else {
		d.Queue.Kill(&job)
	}
}

func (d *dispatcher) Run() {
	d.slots = make(chan struct{}, d.NumWorkers)
	for i := 0; i < d.NumWorkers; i++ {
		d.slots <- struct{}{}
	}

	d.jobs = make(map[int]Job)
	d.cancel = make(chan struct{})
	d.done = make(chan struct{})

	go func() {
		for {
			select {
			case <-d.cancel:
				close(d.done)
				return
			case <-d.slots:
				job, err := d.Queue.Wait()
				if job.ID == 0 || err != nil {
					d.slots <- struct{}{}
					break
				}

				d.jobsLock.Lock()
				d.jobs[job.ID] = job
				d.jobsLock.Unlock()

				go func() {
					err := d.Handler(job)
					if err != nil {
						d.jobFailed(job)
					} else {
						d.Queue.Finish(job)
					}

					d.jobsLock.Lock()
					delete(d.jobs, job.ID)
					d.jobsLock.Unlock()

					// Don't put slot back into pool until job status has been updated
					d.slots <- struct{}{}
				}()
			}
		}
	}()
}
