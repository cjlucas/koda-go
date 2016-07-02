package koda

import (
	"sync"
	"time"
)

// jobManager handles in-flight jobs being processed by the dispatcher. Its main
// purpose is to handle data races during cancellation where both the cancellation
// goroutine and the main run goroutine may attempt to update the state of a single job
type jobManager struct {
	Queue    Queue
	c        *Client
	jobs     map[int]Job
	jobsLock sync.Mutex
}

func (m *jobManager) Add(job Job) {
	m.jobsLock.Lock()
	defer m.jobsLock.Unlock()

	m.jobs[job.ID] = job
}

func (m *jobManager) Success(job Job) {
	m.jobsLock.Lock()
	defer m.jobsLock.Unlock()

	if j, ok := m.jobs[job.ID]; ok {
		m.c.finish(&j)
		delete(m.jobs, job.ID)
	}
}

func (m *jobManager) fail(job Job) {
	if j, ok := m.jobs[job.ID]; ok {
		if job.NumAttempts < m.Queue.MaxAttempts {
			m.c.retry(&j, m.Queue)
		} else {
			m.c.kill(&j)
		}

		delete(m.jobs, job.ID)
	}
}

func (m *jobManager) Fail(job Job) {
	m.jobsLock.Lock()
	defer m.jobsLock.Unlock()

	m.fail(job)
}

func (m *jobManager) FailAllJobs() {
	m.jobsLock.Lock()
	defer m.jobsLock.Unlock()

	var jobs []Job
	for _, job := range m.jobs {
		jobs = append(jobs, job)
	}

	for _, job := range jobs {
		m.fail(job)
	}
}

type dispatcher struct {
	Queue   Queue
	Handler HandlerFunc
	client  *Client

	cancel     chan struct{}
	slots      chan struct{}
	jobManager jobManager
}

// Cancel all running jobs. If timeout is set, will block until
// all outstanding workers have returned. If timeout expires, all pending jobs
// are marked as failed
func (d *dispatcher) Cancel(timeout time.Duration) {
	if timeout > 0 {
		done := make(chan struct{})
		go func() {
			for i := 0; i < d.Queue.NumWorkers; i++ {
				<-d.slots
			}
			done <- struct{}{}
		}()

		select {
		case <-done:
		case <-time.After(timeout):
		}
	}

	d.cancel <- struct{}{}
	<-d.cancel

	d.jobManager.FailAllJobs()
}

func (d *dispatcher) Run() {
	d.slots = make(chan struct{}, d.Queue.NumWorkers)
	for i := 0; i < d.Queue.NumWorkers; i++ {
		d.slots <- struct{}{}
	}

	d.cancel = make(chan struct{})
	d.jobManager = jobManager{
		Queue: d.Queue,
		c:     d.client,
		jobs:  make(map[int]Job),
	}

	go func() {
		for {
			select {
			case <-d.cancel:
				close(d.cancel)
				return
			case <-d.slots:
				job, err := d.client.wait(d.Queue)
				if job.ID == 0 || err != nil {
					d.slots <- struct{}{}
					break
				}

				d.jobManager.Add(job)

				go func() {
					err := d.Handler(job)
					if err != nil {
						d.jobManager.Fail(job)
					} else {
						d.jobManager.Success(job)
					}

					// Don't put slot back into pool until job status has been updated
					d.slots <- struct{}{}
				}()
			}
		}
	}()
}
