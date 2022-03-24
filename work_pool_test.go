package timewheel

import (
	"fmt"
	"testing"
	"time"
)

func TestWorkPool(t *testing.T) {
	pool := NewWorkPool(10)
	pool.Run()

	for i := 0; i < 100; i++ {
		pool.Put(func(args ...interface{}) {
			fmt.Println("111")
			time.Sleep(time.Second * 2)
		})
	}

	select {}
}
