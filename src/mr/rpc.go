package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

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

// Add your RPC definitions here.

type JobType int

const (
	_ JobType = iota
	MapJob
	ReduceJob
	WaitJob
	CompleteJob
)

type TaskStatus int

const (
	_ TaskStatus = iota
	Waiting
	Doing
	Success
)

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

type HeartbeatRequest struct {
}

type HeartbeatResponse struct {
	JobType  JobType
	FileName string
	NReduce  int
	TaskId   int
}

type ReportRequest struct {
	JobType    JobType
	TaskId     int
	TaskStatus TaskStatus
}

type ReportResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
