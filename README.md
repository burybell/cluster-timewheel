# cluster-timewheel
### 介绍
go实现的时间轮，可单机可分布式
### 安装
```cmd
go get github.com/burybell/cluster-timewheel@v1.0.3
```
### 案例
#### 单机
```go
package main

import (
	"fmt"
	timewheel "gitee.com/burybell/cluster-timewheel"
	"time"
)

const (
	CallPrint timewheel.CallId = iota
)

func init() {

	timewheel.AddCall(CallPrint, func(ctx *timewheel.Context) {
		fmt.Println("call1", ctx.Val())
	})
}

func main() {

	local := timewheel.NewLocal(time.Second, 60)

	local.Run()

	for i := 0; i < 1000; i++ {
		context := timewheel.NewContext("cl1")
		local.AddTimer(time.Second*time.Duration(i), fmt.Sprintf("id-%d", i), context, CallPrint)
	}

	select {}
}

```
#### 分布式
```go

package main

import (
	"fmt"
	timewheel "gitee.com/burybell/cluster-timewheel"
	"github.com/go-redis/redis"
	"time"
)

const (
	CallPrint timewheel.CallId = iota
)

func init() {

	timewheel.AddCall(CallPrint, func(ctx *timewheel.Context) {
		fmt.Println("call1", ctx.Val())
	})

}

func main() {

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	cluster := timewheel.NewCluster(client, "wheel-name", nil)

	cluster.Run()

	for i := 0; i < 1000; i++ {
		cluster.AddTimer(time.Second*time.Duration(i), fmt.Sprintf("id-%d", i), nil, CallPrint)
	}

	select {}
}


```