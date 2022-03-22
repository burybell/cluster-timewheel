package timewheel

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"strconv"
	"time"
)

type Options struct {
	Key      string        `json:"key,omitempty"`
	Interval time.Duration `json:"interval,omitempty"`
	SlotNums int           `json:"slotNums,omitempty"`
}

func (options *Options) From(bytes string) {
	_ = json.Unmarshal([]byte(bytes), options)
}

func (options *Options) String() string {
	bytes, err := json.Marshal(options)
	if err != nil {
		return ""
	}
	return string(bytes)
}

type ClusterTimeWheel struct {
	key        string
	interval   time.Duration
	ticker     *time.Ticker
	slotNums   int
	currentPos int
	slots      []string
	client     *redis.Client
	stopChan   chan struct{}
}

func NewCluster(client *redis.Client, options Options) TimeWheel {
	if client == nil || len(options.Key) == 0 {
		return nil
	}

	var wheel ClusterTimeWheel
	wheel.client = client
	wheel.key = options.Key
	wheel.init(&options)
	wheel.interval = options.Interval
	wheel.slotNums = options.SlotNums
	wheel.slots = make([]string, wheel.slotNums)
	wheel.stopChan = make(chan struct{})
	for i := 0; i < wheel.slotNums; i++ {
		wheel.slots[i] = wheel.slotKey(i)
	}
	return &wheel
}

func (wheel *ClusterTimeWheel) init(options *Options) {
	wheel.Tx(func(pipe redis.Pipeliner) {
		res := pipe.Get(options.Key)
		if res.Err() == nil {
			options.From(res.Val())
		} else {
			pipe.Set(options.Key, options.String(), time.Hour*20)
		}
	})
}

func (wheel *ClusterTimeWheel) Tx(exec func(pipe redis.Pipeliner)) {
	pipeline := wheel.client.TxPipeline()
	exec(pipeline)
	_, _ = pipeline.Exec()
}

func (wheel *ClusterTimeWheel) slotKey(index int) string {
	return fmt.Sprintf("%s-slot-%d", wheel.key, index)
}

func (wheel *ClusterTimeWheel) taskKey(id string) string {
	return fmt.Sprintf("%s-task-%s", wheel.key, id)
}

func (wheel *ClusterTimeWheel) locationKey() string {
	return fmt.Sprintf("%s-location", wheel.key)
}

func (wheel *ClusterTimeWheel) foreach(it func(slotKey string, taskKey string)) {
	rge := wheel.client.LRange(wheel.slots[wheel.currentPos], 0, -1)
	if rge.Err() == nil {
		for i := range rge.Val() {
			it(wheel.slots[wheel.currentPos], wheel.taskKey(rge.Val()[i]))
		}
	}
}

func (wheel *ClusterTimeWheel) GetTask(key string) *Task {
	all := wheel.client.HGetAll(key)
	if all.Err() == nil {
		if all.Val()["locker"] == "1" {
			return nil
		}

		task := new(Task)
		for key := range all.Val() {
			switch key {
			case "id":
				task.Id = all.Val()[key]
			case "delay":
				task.Delay, _ = strconv.ParseInt(all.Val()[key], 10, 64)
			case "circle":
				task.Circle, _ = strconv.Atoi(all.Val()[key])
			case "callId":
				callId, _ := strconv.Atoi(all.Val()[key])
				task.CallId = CallId(callId)
			case "context":
				task.Context = GetContext(all.Val()[key])
			}
		}
		return task
	}
	return nil
}

func (wheel *ClusterTimeWheel) AddTimer(delay time.Duration, id string, ctx *Context, callId CallId) {
	if delay < 0 {
		return
	}

	if !CallExist(callId) {
		log.Printf("addtimer call is not exist")
		return
	}

	var task = Task{Id: id, Context: ctx, CallId: callId, Delay: int64(delay.Seconds())}
	task.Circle = int(task.Delay / int64(wheel.interval.Seconds()) / int64(wheel.slotNums))
	index := (wheel.currentPos + int(task.Delay)/int(wheel.interval.Seconds())) % wheel.slotNums
	log.Printf("add index: %d circle: %d", index, task.Circle)
	exists := wheel.client.HExists(wheel.locationKey(), task.Id)
	if exists.Err() != nil {
		log.Printf("add redis err: %+v", exists.Err())
		return
	}

	if exists.Val() {
		log.Printf("add exist err")
		return
	}

	wheel.client.HSet(wheel.locationKey(), task.Id, wheel.slotKey(index))
	wheel.Tx(func(pipe redis.Pipeliner) {
		pipe.RPush(wheel.slots[index], task.Id)
		pipe.HSet(wheel.taskKey(task.Id), "id", task.Id)
		pipe.HSet(wheel.taskKey(task.Id), "delay", task.Delay)
		pipe.HSet(wheel.taskKey(task.Id), "circle", task.Circle)
		pipe.HSet(wheel.taskKey(task.Id), "callId", int(task.CallId))
		pipe.HSet(wheel.taskKey(task.Id), "context", task.Context.String())
	})

}

func (wheel *ClusterTimeWheel) RemoveTimer(id string) {
	exists := wheel.client.HExists(wheel.locationKey(), id)
	if exists.Err() != nil {
		return
	}

	if exists.Val() {
		get := wheel.client.HGet(wheel.locationKey(), id)
		if get.Err() != nil {
			return
		}

		wheel.Tx(func(pipe redis.Pipeliner) {
			pipe.LRem(get.Val(), 0, id)
			pipe.Del(wheel.taskKey(id))
			pipe.HDel(wheel.locationKey(), id)
		})
	}
}

func (wheel *ClusterTimeWheel) run() {
	wheel.ticker = time.NewTicker(wheel.interval)
	for {
		select {
		case <-wheel.ticker.C:
			wheel.execCallback()
		case <-wheel.stopChan:
			return
		}
	}
}

func (wheel *ClusterTimeWheel) execCallback() {
	wheel.foreach(func(slotKey string, taskKey string) {
		task := wheel.GetTask(taskKey)
		if task != nil {
			nx := wheel.client.HSetNX(taskKey, "locker", "1")
			if nx.Err() != nil || !nx.Val() {
				return
			}

			if task.Circle > 0 {
				wheel.client.HIncrBy(taskKey, "circle", -1)
				wheel.client.HDel(taskKey, "locker")
				return
			}
			log.Printf("task %+v", task)
			if CallExist(task.CallId) {
				go func(t *Task) {
					defer func() {
						if err := recover(); err != nil {
							log.Printf("exec task: %+v err: %+v", t, err)
						}
					}()
					GetCall(task.CallId)(task.Context)
				}(task)
			}
			wheel.client.HDel(wheel.locationKey(), task.Id)
			wheel.client.LRem(slotKey, 0, task.Id)
			wheel.client.Del(taskKey)
		}
	})

	if wheel.currentPos == wheel.slotNums-1 {
		wheel.currentPos = 0
	} else {
		wheel.currentPos++
	}
}

func (wheel *ClusterTimeWheel) Run() TimeWheel {
	go wheel.run()
	return wheel
}

func (wheel *ClusterTimeWheel) Stop() {
	wheel.stopChan <- struct{}{}
}
