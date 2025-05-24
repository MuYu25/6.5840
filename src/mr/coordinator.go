package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Assigned TaskStatus = iota
	Unassigned
	Completed
	Fialed
)

type TaskInfo struct {
	FileName   string
	TaskStatus TaskStatus
	TimeStamp  time.Time
}

type Coordinator struct {
	// Your definitions here.
	Mutex              sync.Mutex
	AllMapCompleted    bool
	AllReduceCompleted bool
	MapTasks           []TaskInfo
	ReduceTasks        []TaskInfo
	NMap               int
	NReduce            int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.

	return c.AllReduceCompleted
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:            nReduce,
		NMap:               len(files),
		MapTasks:           make([]TaskInfo, len(files)),
		ReduceTasks:        make([]TaskInfo, nReduce),
		AllMapCompleted:    false,
		AllReduceCompleted: false,
		Mutex:              sync.Mutex{},
	}

	// Your code here.
	c.InitTask(files)
	c.server()
	return &c
}

func (c *Coordinator) InitTask(file []string) {
	for idx := range file {
		c.MapTasks[idx] = TaskInfo{
			FileName:   file[idx],
			TaskStatus: Unassigned,
			TimeStamp:  time.Now(),
		}
	}
	for idx := range c.ReduceTasks {
		c.ReduceTasks[idx] = TaskInfo{
			TaskStatus: Unassigned,
		}
	}
}

func (c *Coordinator) RequestTask(args *MessageSend, reply *MessageReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if !c.AllMapCompleted {
		NMapCompleted := 0
		for idx := range c.MapTasks {
			if c.MapTasks[idx].TaskStatus == Unassigned ||
				c.MapTasks[idx].TaskStatus == Fialed ||
				(c.MapTasks[idx].TaskStatus == Assigned && time.Since(c.MapTasks[idx].TimeStamp) > 10*time.Second) {
				reply.FileName = c.MapTasks[idx].FileName
				reply.TaskID = idx
				reply.TaskType = MapTask
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce
				c.MapTasks[idx].TaskStatus = Assigned
				c.MapTasks[idx].TimeStamp = time.Now()
				return nil
			} else if c.MapTasks[idx].TaskStatus == Completed {
				NMapCompleted++
			}
		}
		if NMapCompleted == len(c.MapTasks) {
			c.AllMapCompleted = true
		} else {
			reply.TaskType = Wait
			return nil
		}
	}
	if !c.AllReduceCompleted {
		NReduceCompleted := 0
		for idx := range c.ReduceTasks {
			if c.ReduceTasks[idx].TaskStatus == Unassigned ||
				c.ReduceTasks[idx].TaskStatus == Fialed ||
				(c.ReduceTasks[idx].TaskStatus == Assigned && time.Since(c.ReduceTasks[idx].TimeStamp) > 10*time.Second) {
				reply.TaskID = idx
				reply.TaskType = ReduceTask
				reply.FileName = c.ReduceTasks[idx].FileName
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce
				c.ReduceTasks[idx].TaskStatus = Assigned
				c.ReduceTasks[idx].TimeStamp = time.Now()
				return nil
			} else if c.ReduceTasks[idx].TaskStatus == Completed {
				NReduceCompleted++
			}
		}
		if NReduceCompleted == len(c.ReduceTasks) {
			c.AllReduceCompleted = true
		} else {
			reply.TaskType = Wait
			return nil
		}
	}
	reply.TaskType = Exit

	return nil
}

func (c *Coordinator) ReportTask(args *MessageSend, reply *MessageReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if args.CompleteStatus == MapTaksComplete {
		c.MapTasks[args.TaskID].TaskStatus = Completed
	} else if args.CompleteStatus == MapTaskFailed {
		c.MapTasks[args.TaskID].TaskStatus = Fialed
	} else if args.CompleteStatus == ReduceTaskComplete {
		c.ReduceTasks[args.TaskID].TaskStatus = Completed
	} else if args.CompleteStatus == ReduceTaskFailed {
		c.ReduceTasks[args.TaskID].TaskStatus = Fialed
	}

	return nil
}
