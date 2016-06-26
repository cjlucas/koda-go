package koda

import "time"

func GetQueue(name string) *Queue {
	return DefaultClient.GetQueue(name)
}

func Submit(queue string, priority int, payload interface{}) (*Job, error) {
	return DefaultClient.GetQueue(queue).Submit(priority, payload)
}

func SubmitDelayed(queue string, d time.Duration, payload interface{}) (*Job, error) {
	return DefaultClient.GetQueue(queue).SubmitDelayed(d, payload)
}

func Register(queue string, numWorkers int, f HandlerFunc) {
	DefaultClient.Register(queue, numWorkers, f)
}

func Work() Canceller {
	return DefaultClient.Work()
}

func WorkForever() {
	DefaultClient.WorkForever()
}
