package timewheel

import (
	"encoding/json"
	"sync"
	"time"
)

// Context 定时器上下文
type Context struct {
	Name string                 `json:"name,omitempty"`
	Data map[string]interface{} `json:"data,omitempty"`
}

func NewContext(name string) *Context {
	return &Context{
		Name: name,
		Data: make(map[string]interface{}),
	}
}

func (c Context) Val() string {
	bytes, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return string(bytes)
}

func getContext(bytes string) *Context {
	ctx := Context{}
	_ = json.Unmarshal([]byte(bytes), &ctx)
	return &ctx
}

type TimeWheel interface {
	AddTimer(delay time.Duration, id string, ctx *Context, callId CallId)
	RemoveTimer(id string)
	Run() TimeWheel
	Stop()
}

type CallId int

type Call func(ctx *Context)

type CallPool struct {
	calls map[CallId]Call
}

func (pool *CallPool) Add(id CallId, call Call) {
	pool.calls[id] = call
}

func (pool *CallPool) Remove(id CallId) {
	delete(pool.calls, id)
}

func (pool *CallPool) Exist(id CallId) bool {
	if _, ok := pool.calls[id]; ok {
		return true
	}
	return false
}

var pool = CallPool{calls: make(map[CallId]Call)}

// AddCall 添加全剧回调函数
func AddCall(id CallId, call Call) {
	pool.Add(id, call)
}

func RemoveCall(id CallId) {
	pool.Remove(id)
}

func GetCall(id CallId) Call {
	return pool.calls[id]
}

func CallExist(id CallId) bool {
	return pool.Exist(id)
}

type Target struct {
	Id      string   `json:"id"`
	Delay   int64    `json:"delay"`
	Circle  int      `json:"circle"`
	Context *Context `json:"context"`
	CallId  CallId   `json:"callId"`
}

type Node struct {
	Element *Target // 任务
	Next    *Node   // 下一个节点
}

type OrderList struct {
	Header *Node
	locker sync.Mutex
}

func NewOrderList() *OrderList {
	return &OrderList{Header: new(Node)}
}

func (list *OrderList) Add(task *Target) {
	var ptr = list.Header
	for {
		if ptr.Next == nil {
			ptr.Next = &Node{
				Element: task,
				Next:    nil,
			}
			return
		} else {
			if ptr.Next.Element.Circle > task.Circle {
				tmp := ptr.Next
				ptr.Next = &Node{
					Element: task,
					Next:    nil,
				}
				ptr.Next.Next = tmp
				return
			} else {
				ptr = ptr.Next
			}
		}
	}
}

func (list *OrderList) Remove(task *Target) {
	var ptr = list.Header
	for {
		if ptr.Next != nil {
			if ptr.Next.Element.Id == task.Id {
				list.locker.Lock()
				ptr.Next = ptr.Next.Next
				list.locker.Unlock()
				return
			} else {
				ptr = ptr.Next
			}
		} else {
			return
		}
	}
}

func (list *OrderList) Foreach(each func(task *Target)) {
	var ptr = list.Header
	for {
		if ptr.Next != nil {
			each(ptr.Next.Element)
			ptr = ptr.Next
		} else {
			return
		}
	}
}

func (list *OrderList) Consumer(each func(task *Target) bool) {
	var ptr = list.Header
	var del = false
	list.locker.Lock()
	defer list.locker.Unlock()
	for {
		if ptr.Next != nil {
			// ptr ptr.next ptr.next.next
			del = each(ptr.Next.Element)
			if del {
				ptr.Next = ptr.Next.Next
			} else {
				ptr = ptr.Next
			}
		} else {
			return
		}
	}
}
