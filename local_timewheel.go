package timewheel

import (
	"log"
	"time"
)

// LocalTimeWheel 基于本地内存实现
type LocalTimeWheel struct {
	interval   time.Duration
	ticker     *time.Ticker
	slots      []*OrderList
	location   map[string]int
	currentPos int
	slotNums   int
	addChan    chan Target
	removeChan chan string
	stopChan   chan struct{}
}

func NewLocal(interval time.Duration, slotNums ...int) TimeWheel {
	if interval <= 0 {
		return nil
	}

	wheel := &LocalTimeWheel{interval: interval}
	if len(slotNums) > 0 {
		wheel.slotNums = slotNums[0]
	} else {
		wheel.slotNums = 60
	}

	wheel.addChan = make(chan Target, 10)
	wheel.removeChan = make(chan string)
	wheel.stopChan = make(chan struct{})
	wheel.location = make(map[string]int)
	wheel.slots = make([]*OrderList, wheel.slotNums)
	for i := 0; i < wheel.slotNums; i++ {
		wheel.slots[i] = NewOrderList()
	}

	return wheel
}

func (wheel *LocalTimeWheel) run() {
	wheel.ticker = time.NewTicker(wheel.interval)
	for {
		select {
		case <-wheel.ticker.C:
			wheel.execCallback()
		case task := <-wheel.addChan:
			wheel.addCallback(task)
		case id := <-wheel.removeChan:
			wheel.removeCallback(id)
		case <-wheel.stopChan:
			return
		}
	}
}

func (wheel *LocalTimeWheel) Run() TimeWheel {
	go wheel.run()
	return wheel
}

func (wheel *LocalTimeWheel) AddTimer(delay time.Duration, id string, ctx *Context, callId CallId) {
	if delay < 0 {
		return
	}

	if index, ok := wheel.location[id]; ok {
		log.Printf("addtimer exist in %d", index)
		return
	}

	if !CallExist(callId) {
		log.Printf("addtimer call is not exist")
		return
	}

	var task = Target{Id: id, Context: ctx, CallId: callId, Delay: int64(delay.Seconds())}

	wheel.addChan <- task
}

func (wheel *LocalTimeWheel) addCallback(task Target) {
	task.Circle = int(task.Delay / int64(wheel.interval.Seconds()) / int64(wheel.slotNums))
	index := (wheel.currentPos + int(task.Delay)/int(wheel.interval.Seconds())) % wheel.slotNums
	log.Printf("add index: %d circle: %d", index, task.Circle)
	wheel.slots[index].Add(&task)
	wheel.location[task.Id] = index
}

func (wheel *LocalTimeWheel) removeCallback(id string) {
	if pos, ok := wheel.location[id]; ok {
		wheel.slots[pos].Remove(&Target{Id: id})
	}
}

func (wheel *LocalTimeWheel) execCallback() {
	wheel.slots[wheel.currentPos].Consumer(func(task *Target) bool {
		if task.Circle > 0 {
			task.Circle--
			return false
		}
		log.Printf("task %+v", task)
		if CallExist(task.CallId) {
			go func(t *Target) {
				defer func() {
					if err := recover(); err != nil {
						log.Printf("exec task: %+v err: %+v", t, err)
					}
				}()
				GetCall(task.CallId)(task.Context)
			}(task)
		}
		delete(wheel.location, task.Id)
		return true
	})

	if wheel.currentPos == wheel.slotNums-1 {
		wheel.currentPos = 0
	} else {
		wheel.currentPos++
	}
}

func (wheel *LocalTimeWheel) RemoveTimer(id string) {
	wheel.removeChan <- id
}

func (wheel LocalTimeWheel) Stop() {
	wheel.stopChan <- struct{}{}
}
