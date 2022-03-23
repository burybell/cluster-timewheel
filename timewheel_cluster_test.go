package timewheel

import (
	"fmt"
	"github.com/go-redis/redis"
	"math/rand"
	"testing"
	"time"
)

func TestCluster(t *testing.T) {

	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	wheel := NewCluster(client, "redis-wheel-7", nil)

	wheel.Run()

	for i := 0; i < 10; i++ {
		tp := Call1
		if i%2 == 0 {
			tp = Call2
		}
		wheel.AddTimer(time.Second*time.Duration(i), fmt.Sprintf("id-%d", i), nil, tp)
	}
	wheel.AddTimer(time.Second*2, "1", nil, Call1)
	wheel.AddTimer(time.Second*5, "3", nil, Call2)
	wheel.AddTimer(time.Second*7, "7", nil, Call3)
	wheel.RemoveTimer("3")
	select {}

}

func TestClusterClient1(t *testing.T) {

	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	wheel := NewCluster(client, "redis-wheel-5", nil)

	wheel.Run()

	for i := 0; i < 1000; i++ {
		tp := Call1
		if i%2 == 0 {
			tp = Call2
		}
		ctx := NewContext("cl1")
		wheel.AddTimer(time.Second*time.Duration(rand.Int63n(100)), fmt.Sprintf("id-%d", i), ctx, tp)
	}
	select {}

}

func TestClusterClient2(t *testing.T) {

	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	wheel := NewCluster(client, "redis-wheel-5", nil)

	wheel.Run()

	for i := 1000; i < 2000; i++ {
		tp := Call1
		if i%2 == 0 {
			tp = Call2
		}
		ctx := NewContext("cl2")
		wheel.AddTimer(time.Second*time.Duration(rand.Int63n(100)), fmt.Sprintf("id-%d", i), ctx, tp)
	}
	select {}

}
