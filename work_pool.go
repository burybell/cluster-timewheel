package timewheel

import "sync"

type target struct {
	call func(args ...interface{})
	args []interface{}
}

type workPool struct {
	signal   chan struct{}
	stop     chan struct{}
	capacity int
	targets  []*target
	locker   sync.Mutex
}

func NewWorkPool(capacity ...int) *workPool {
	var cap = 10
	if len(capacity) > 0 {
		cap = capacity[0]
	}
	return &workPool{
		signal:   make(chan struct{}, cap),
		stop:     make(chan struct{}, 0),
		capacity: cap,
		targets:  make([]*target, 0),
		locker:   sync.Mutex{},
	}
}

func (wp *workPool) Put(call func(args ...interface{}), args ...interface{}) {
	wp.locker.Lock()
	wp.targets = append(wp.targets, &target{call: call, args: args})
	wp.locker.Unlock()
}

func (wp *workPool) run() {
	for {
		select {
		case <-wp.signal:
			if len(wp.targets) > 0 {
				op := wp.targets[0]
				wp.locker.Lock()
				wp.targets = wp.targets[1:]
				wp.locker.Unlock()
				go func(target *target) {
					target.call(target.args...)
					wp.signal <- struct{}{}
				}(op)
			}
		case <-wp.stop:
			return
		}
	}
}

func (wp *workPool) Run() {
	for i := 0; i < wp.capacity; i++ {
		wp.signal <- struct{}{}
	}
	go wp.run()
}

func (wp *workPool) Stop() {
	wp.stop <- struct{}{}
}
