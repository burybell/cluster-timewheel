package timewheel

import (
	"fmt"
	"testing"
	"time"
)

const (
	Call1 CallId = iota
	Call2
	Call3
)

func init() {
	AddCall(Call1, func(ctx *Context) {
		fmt.Println("call1", ctx.Name)
		time.Sleep(time.Second * 10)
	})

	AddCall(Call2, func(ctx *Context) {
		fmt.Println("call2", ctx.Name)
		time.Sleep(time.Second * 10)
		panic("run")
	})

	AddCall(Call3, func(ctx *Context) {
		fmt.Println("call3", ctx.Name)
		time.Sleep(time.Second * 10)
	})
}

func TestLocal(t *testing.T) {
	wheel := NewLocal(time.Second, 60)
	wheel.Run()
	for i := 0; i < 10; i++ {
		tp := Call1
		if i%2 == 0 {
			tp = Call2
		}
		wheel.AddTimer(time.Second*time.Duration(i), fmt.Sprintf("id-%d", i), nil, tp)
	}
	wheel.AddTimer(time.Second*3, "1", nil, Call1)

	wheel.AddTimer(time.Second*20, "7", nil, Call3)
	after := time.After(time.Second * 3)
	select {
	case <-after:
		wheel.RemoveTimer("7")
	}
	select {}
}
