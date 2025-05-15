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
	Unassigned = iota
	Assigned
	Completed
	Failed
)

type TaskInfo struct {
	TaskID    int
	TaskType  TaskStatus
	TaskFile  string
	TimeStamp time.Time
}

type Coordinator struct {
	// Your definitions here.
	NMap                    int
	NReduce                 int
	MapTasks                []TaskInfo
	ReduceTasks             []TaskInfo
	AllMapTasksCompleted    bool
	AllReduceTasksCompleted bool
	Mutex                   sync.Mutex
}

func (s *Coordinator) InitTask(files []string) {
	for idx := range files {
		s.MapTasks[idx] = TaskInfo{
			TaskFile:  files[idx],
			TaskType:  Unassigned,
			TimeStamp: time.Now(),
		}
	}
	for idx := range files {
		s.ReduceTasks[idx] = TaskInfo{
			TaskType: Unassigned,
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *MessageSend, reply *MessageReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if !c.AllMapTasksCompleted {
		NMapTaskCompleted := 0
		for i, task := range c.MapTasks {
			if task.TaskType == Unassigned ||
				task.TaskType == Failed ||
				(task.TaskType == Assigned && time.Since(task.TimeStamp) > 10*time.Second) {
				reply.TaskType = Assigned
				reply.TaskID = i
				reply.FileName = task.TaskFile
				reply.NReduce = c.NReduce
				reply.NMap = c.NMap
				c.MapTasks[i].TaskType = Assigned
				c.MapTasks[i].TimeStamp = time.Now()
			} else if task.TaskType == Completed {
				NMapTaskCompleted++
			}
		}
		if NMapTaskCompleted == len(c.MapTasks) {
			c.AllMapTasksCompleted = true
		} else {
			reply.TaskType = Wait
			return nil
		}
	}
	if !c.AllReduceTasksCompleted {
		NReduceTaskCompleted := 0
		for i, task := range c.ReduceTasks {
			if task.TaskType == Unassigned ||
				task.TaskType == Failed ||
				(task.TaskType == Assigned && time.Since(task.TimeStamp) > 10*time.Second) {
				reply.TaskType = Assigned
				reply.TaskID = i
				reply.FileName = task.TaskFile
				reply.NReduce = c.NReduce
				reply.NMap = c.NMap
				c.ReduceTasks[i].TaskType = Assigned
				c.ReduceTasks[i].TimeStamp = time.Now()
			} else if task.TaskType == Completed {
				NReduceTaskCompleted++
			}
		}
		if NReduceTaskCompleted == len(c.ReduceTasks) {
			c.AllReduceTasksCompleted = true
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
	if args.TaskCompletedStatus == MapTaskCompelete {
		c.MapTasks[args.TaskID].TaskType = Completed
	} else if args.TaskCompletedStatus == MapTaskFailed {
		c.MapTasks[args.TaskID].TaskType = Failed
	} else if args.TaskCompletedStatus == ReduceTaskComplete {
		c.ReduceTasks[args.TaskID].TaskType = Completed
	} else if args.TaskCompletedStatus == ReduceTaskFailed {
		c.ReduceTasks[args.TaskID].TaskType = Failed
	}
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:                 nReduce,
		NMap:                    len(files),
		MapTasks:                make([]TaskInfo, len(files)),
		ReduceTasks:             make([]TaskInfo, nReduce),
		AllMapTasksCompleted:    false,
		AllReduceTasksCompleted: false,
	}
	c.InitTask(files)

	// Your code here.

	c.server()
	return &c
}
