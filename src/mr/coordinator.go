package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	lock           sync.Mutex
	state          int
	nMap           int
	nReduce        int
	nextId         int
	tasks          map[string]Task
	availableTasks chan Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if args.TaskType != NULL {
		c.lock.Lock()
		lastTaskId := taskName(args.TaskType, args.TaskId)
		task, ok := c.tasks[lastTaskId]
		if ok && task.workId == args.WorkId {
			if args.TaskType == MAP {
				for i := 0; i < c.nReduce; i++ {
					os.Rename(tmpMapOutFile(args.WorkId, args.TaskId, i), finalMapOutFile(args.TaskId, i))
				}
			} else if args.TaskType == REDUCE {
				os.Rename(tmpReduceOutFile(args.WorkId, args.TaskId), finalReduceOutFile(args.TaskId))
			}
			delete(c.tasks, lastTaskId)
			if len(c.tasks) == 0 {
				c.changeState()
			}
		}
		c.lock.Unlock()
	}
	task, ok := <-c.availableTasks
	if !ok {
		reply.TaskType = NULL
		return nil
	}
	c.lock.Lock()
	if args.WorkId == NULL {
		args.WorkId = c.nextId
		c.nextId++
	}
	task.workId = args.WorkId
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[taskName(task.taskType, task.fileIndex)] = task
	reply.TaskId = task.fileIndex
	reply.TaskType = task.taskType
	reply.WorkId = task.workId
	reply.File = task.mapFile
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) changeState() {
	if c.state == MAP {
		c.state = REDUCE
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				taskType:  REDUCE,
				fileIndex: i,
			}
			c.tasks[taskName(REDUCE, i)] = task
			c.availableTasks <- task
		}

	} else if c.state == REDUCE {
		c.state = END
		close(c.availableTasks)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.state == END
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		state:          MAP,
		nMap:           len(files),
		nReduce:        nReduce,
		nextId:         1,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	for i, file := range files {
		task := Task{
			taskType:  MAP,
			fileIndex: i,
			mapFile:   file,
			workId:    NULL,
		}
		c.tasks[taskName(MAP, i)] = task
		c.availableTasks <- task
	}
	// Your code here.

	c.server()
	go func() {
		for {
			time.Sleep(1 * time.Second)
			c.lock.Lock()
			for _, task := range c.tasks {
				if task.workId != NULL && time.Now().After(task.Deadline) {
					task.workId = NULL
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}
