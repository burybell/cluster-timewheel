# cluster-timewheel

### 介绍
go实现的时间轮，可单机可分布式  

### 案例

### 回调函数

```go

const (
    Call1 CallId = iota
    Call2
    Call3
)

func init() {
    AddCall(Call1, func(ctx *Context) {
        fmt.Println("call1", ctx.Name)
    })
    
    AddCall(Call2, func(ctx *Context) {
        fmt.Println("call2", ctx.Name)
        panic("run")
    })
    
    AddCall(Call3, func(ctx *Context) {
        fmt.Println("call3", ctx.Name)
    })
}

```

#### 单机
```go

func main() {
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
```

#### 分布式
```go

func main() {

    client := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379",
    })

    wheel := NewCluster(client, Options{
        Key:      "redis-wheel",
        Interval: time.Second,
        SlotNums: 60,
    })

    wheel.Run()

    for i := 0; i < 1000; i++ {
        tp := Call1
        if i%2 == 0 {
        tp = Call2
        }
        ctx := NewContext("cl1")
        wheel.AddTimer(time.Second*time.Duration(i), fmt.Sprintf("id-%d", i), ctx, tp)
    }
    select {}
	
}

```