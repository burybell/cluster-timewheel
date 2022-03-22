package timewheel

import (
	"fmt"
	"testing"
)

func TestOrderListAdd(t *testing.T) {

	list := NewOrderList()

	list.Add(&Task{
		Id:     "1",
		Circle: 1,
	})

	list.Add(&Task{
		Id:     "2",
		Circle: 4,
	})

	list.Add(&Task{
		Id:     "3",
		Circle: 2,
	})

	list.Add(&Task{
		Id:     "4",
		Circle: 3,
	})

	// id: 1,3,4,2

	list.Foreach(func(task *Task) {
		fmt.Println(task.Id)
	})

}

func TestOrderListRemove(t *testing.T) {

	list := NewOrderList()

	list.Add(&Task{
		Id:     "1",
		Circle: 1,
	})

	list.Add(&Task{
		Id:     "2",
		Circle: 4,
	})

	list.Add(&Task{
		Id:     "3",
		Circle: 2,
	})

	list.Add(&Task{
		Id:     "4",
		Circle: 3,
	})

	// id: 1,3,4,2

	list.Foreach(func(task *Task) {
		fmt.Println(task.Id)
	})

	list.Remove(&Task{
		Id:     "3",
		Circle: 2,
	})

	fmt.Println("remove after")
	list.Foreach(func(task *Task) {
		fmt.Println(task.Id)
	})

}

func TestOrderListConsumer(t *testing.T) {

	list := NewOrderList()

	list.Add(&Task{
		Id:     "1",
		Circle: 1,
	})

	list.Add(&Task{
		Id:     "2",
		Circle: 4,
	})

	list.Add(&Task{
		Id:     "3",
		Circle: 2,
	})

	list.Add(&Task{
		Id:     "4",
		Circle: 3,
	})

	// id: 1,3,4,2

	list.Foreach(func(task *Task) {
		fmt.Println(task.Id)
	})

	fmt.Println("consumer ing")
	list.Consumer(func(task *Task) bool {
		fmt.Println(task.Id)
		if task.Id == "2" {
			return false
		}
		return true
	})

	fmt.Println("consumer after")
	list.Foreach(func(task *Task) {
		fmt.Println(task.Id)
	})

}
