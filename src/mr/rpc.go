package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	MAP    = 1
	REDUCE = 2
	NULL   = 0
	END    = -1
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

//gob: type mr.RequestTaskArgs has no exported fields
//原因，使用外部包的时候，如果结构体的字段名应该大写开头，否则会报错
type RequestTaskArgs struct {
	WorkId int
	// 对map任务来说，TaskId是文件的索引，对reduce任务来说，TaskId是reduce的索引
	TaskId   int
	TaskType int
}

type RequestTaskReply struct {
	TaskId    int
	TaskType  int
	MapNum    int
	ReduceNum int
	File      string
}

type Task struct {
	taskType  int
	fileIndex int
	mapFile   string
	workid    int
	Deadline  time.Time
}

func taskName(taskType int, fileIndex int) string {
	return fmt.Sprintf("%d-%d", taskType, fileIndex)
}

func tmpMapOutFile(worker int, mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("tmp-%d-%d-%d", worker, mapIndex, reduceIndex)
}

func finalMapOutFile(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func tmpReduceOutFile(worker int, reduceIndex int) string {
	return fmt.Sprintf("tmp-out-%d-%d", worker, reduceIndex)
}

func finalReduceOutFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}
