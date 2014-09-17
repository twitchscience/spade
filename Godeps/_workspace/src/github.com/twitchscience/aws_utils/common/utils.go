package common

import "time"

type Retrier struct {
	Times         int
	BackoffFactor int
}

func (r *Retrier) Retry(fn func() error) error {
	var err error
	for i := 1; i <= r.Times; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(i*r.BackoffFactor) * time.Second)
	}
	return err
}
