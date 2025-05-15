package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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

type TaskCompleteStatus int

const (
	MapTaskCompelete = iota
	MapTaskFailed
	ReduceTaskComplete
	ReduceTaskFailed
)

type TaskType int

const (
	MapTask = iota
	ReduceTask
	Wait
	Exit
)

type MessageSend struct {
	TaskID              int
	TaskCompletedStatus TaskCompleteStatus
}

type MessageReply struct {
	TaskID   int
	TaskType TaskType
	FileName string
	NReduce  int // reduce number
	NMap     int // map number
}

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
