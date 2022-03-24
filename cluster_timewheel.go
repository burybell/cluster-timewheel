package timewheel

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"strings"
	"time"
)

type Options struct {
	Interval time.Duration `json:"interval,omitempty"`
	SlotNums int           `json:"slotNums,omitempty"`
	MergeVal bool          `json:"merge,omitempty"`
}

func (options *Options) normalize() {
	options.Interval = time.Second
	options.SlotNums = 60
	options.MergeVal = false
}

func (options *Options) Val() string {
	return fmt.Sprintf("%d-%d", int64(options.Interval.Seconds()), options.SlotNums)
}

func newOptions(value string) *Options {
	options := new(Options)
	if val := strings.Split(value, "-"); len(val) == 2 {
		options.Interval = time.Second * time.Duration(Int(val[0]))
		options.SlotNums = Int(val[1])
	} else {
		options.normalize()
	}
	return options
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

func NewCluster(client *redis.Client, key string, options *Options) TimeWheel {
	if client == nil || len(key) == 0 {
		return nil
	}

	if options == nil {
		options = new(Options)
		options.normalize()
	}

	if options.MergeVal {
		client.Set(key, options.Val(), -1)
	} else {
		if val := client.Get(key); val.Err() == nil && len(val.Val()) > 0 {
			options = newOptions(val.Val())
		} else {
			client.Set(key, options.Val(), -1)
		}
	}

	var wheel ClusterTimeWheel
	wheel.client = client
	wheel.key = key
	wheel.interval = options.Interval
	wheel.slotNums = options.SlotNums
	wheel.slots = make([]string, wheel.slotNums)
	wheel.stopChan = make(chan struct{})
	for i := 0; i < wheel.slotNums; i++ {
		wheel.slots[i] = fmt.Sprintf("%s-slot-%d", wheel.key, i)
	}
	return &wheel
}

// putTarget PutTask序列化任务
func (wheel *ClusterTimeWheel) putTarget(target *Target) bool {
	if !wheel.lockTarget(target.Id) {
		return false
	}

	key := fmt.Sprintf("%s-target-%s", wheel.key, target.Id)
	pipe := wheel.client.TxPipeline()
	pipe.HSet(key, "ID", target.Id)
	pipe.HSet(key, "DELAY", target.Delay)
	pipe.HSet(key, "CIRCLE", target.Circle)
	pipe.HSet(key, "CONTEXT", target.Context.Val())
	pipe.HSet(key, "CALL_ID", int(target.CallId))
	pipe.HSet(key, "DYNAMIC", target.Circle)
	pipe.HDel(key, "LOCK")
	if _, err := pipe.Exec(); err != nil {
		wheel.unlockTarget(target.Id)
		return false
	}
	return true
}

//getTarget 获取任务
func (wheel *ClusterTimeWheel) getTarget(id string) (target *Target) {
	key := fmt.Sprintf("%s-target-%s", wheel.key, id)
	if tag := wheel.client.HGetAll(key); tag.Err() != nil || len(tag.Val()) == 0 {
		return nil
	} else {
		kvs := tag.Val()
		if val, ok := kvs["LOCK"]; ok && val == "LOCK" {
			return nil
		}
		target = new(Target)
		target.Id = kvs["ID"]
		target.Circle = Int(kvs["DYNAMIC"])
		target.Delay = Int64(kvs["DELAY"])
		target.CallId = CallId(Int(kvs["CALL_ID"]))
		target.Context = getContext(kvs["CONTEXT"])
		return target
	}
}

func (wheel *ClusterTimeWheel) delTarget(id string) {
	key := fmt.Sprintf("%s-target-%s", wheel.key, id)
	wheel.client.Del(key)
}

func (wheel *ClusterTimeWheel) lockTarget(id string) bool {
	key := fmt.Sprintf("%s-target-%s", wheel.key, id)
	if nx := wheel.client.HSetNX(key, "LOCK", "LOCK"); nx.Err() != nil || !nx.Val() {
		return false
	}
	return true
}

func (wheel ClusterTimeWheel) decrTarget(id string) {
	key := fmt.Sprintf("%s-target-%s", wheel.key, id)
	wheel.client.HIncrBy(key, "DYNAMIC", -1)
}

func (wheel *ClusterTimeWheel) unlockTarget(id string) {
	key := fmt.Sprintf("%s-target-%s", wheel.key, id)
	wheel.client.HDel(key, "LOCK")
}

func (wheel *ClusterTimeWheel) markLocation(id string, index int) bool {
	key := fmt.Sprintf("%s-location", wheel.key)
	val := fmt.Sprintf("%s-slot-%d", wheel.key, index)
	if nx := wheel.client.HSetNX(key, id, val); nx.Err() != nil || !nx.Val() {
		return false
	}
	return true
}

func (wheel *ClusterTimeWheel) UnMarkLocation(id string) {
	key := fmt.Sprintf("%s-location", wheel.key)
	wheel.client.HDel(key, id)
}

func (wheel *ClusterTimeWheel) getLocation(id string) (location string) {
	key := fmt.Sprintf("%s-location", wheel.key)
	if get := wheel.client.HGet(key, id); get.Err() != nil {
		return ""
	} else {
		return get.Val()
	}
}

func (wheel *ClusterTimeWheel) proxy(it func(id string)) {
	rge := wheel.client.LRange(wheel.slots[wheel.currentPos], 0, -1)
	if rge.Err() == nil {
		for i := range rge.Val() {
			it(rge.Val()[i])
		}
	}
}

func (wheel *ClusterTimeWheel) AddTimer(delay time.Duration, id string, ctx *Context, callId CallId) {
	if delay < 0 {
		return
	}

	if !CallExist(callId) {
		return
	}

	if ctx == nil {
		ctx = NewContext(id)
	}

	target := new(Target)
	target.Id = id
	target.Context = ctx
    target.CallId = callId
	target.Delay = int64(delay.Seconds())
	target.Circle = int(target.Delay / int64(wheel.interval.Seconds()) / int64(wheel.slotNums))
	index := (wheel.currentPos + int(target.Delay)/int(wheel.interval.Seconds())) % wheel.slotNums
	if wheel.markLocation(target.Id, index) {
		wheel.putTarget(target)
		wheel.client.RPush(wheel.slots[index], id)
	}
}

func (wheel *ClusterTimeWheel) RemoveTimer(id string) {
	if location := wheel.getLocation(id); location != "" {
		wheel.client.LRem(location, 0, id)
		wheel.UnMarkLocation(id)
		wheel.delTarget(id)
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
	wheel.proxy(func(id string) {
		target := wheel.getTarget(id)
		if target == nil {
			return
		}

		if !wheel.lockTarget(id) {
			return
		}

		if target.Circle > 0 {
			wheel.decrTarget(id)
			wheel.unlockTarget(id)
			return
		}

		defer func() {
			if location := wheel.getLocation(id); location != "" {
				wheel.client.LRem(location, 0, id)
				wheel.UnMarkLocation(id)
				wheel.delTarget(id)
				wheel.unlockTarget(id)
			}
		}()
		if CallExist(target.CallId) {
			go func(t *Target) {
				defer func() {
					if err := recover(); err != nil {
						log.Printf("exec task: %+v err: %+v", t, err)
					}
				}()
				GetCall(t.CallId)(t.Context)
			}(target)
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
