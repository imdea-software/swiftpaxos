package curp

import (
	"sync"
	"time"
)

type Timer struct {
	c       chan bool
	s       chan int
	wg      sync.WaitGroup
	version int
}

func NewTimer() *Timer {
	return &Timer{
		c:       make(chan bool, 1),
		s:       make(chan int, 1),
		version: 0,
	}
}

func (t *Timer) Start(wait time.Duration) {
	t.wg.Add(1)
	go func(version int, s chan int) {
		first := true
		for {
			if first {
				t.wg.Done()
				first = false
			}
			select {
			case <-s:
				return
			case <-time.After(wait):
				stop := (len(s) != 0)
				t.c <- !stop
				if stop {
					return
				}
			}
		}
	}(t.version, t.s)
}

func (t *Timer) Reset(wait time.Duration) {
	t.Stop()
	t.Start(wait)
}

func (t *Timer) Stop() {
	t.s <- t.version
	t.wg.Wait()
	t.version++
	t.s = make(chan int, 1)
}
