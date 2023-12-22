package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
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
type GetWorkArgs struct {
	WorkerN int
}

type GetWorkReply struct {
	WorkType        string
	WorkKey         string
	NReduce         int
	AssignedWorkerN int
	ReduceBatch     int
}

type ReportWorkDoneArgs struct {
	WorkType  string
	WorkKey   string
	WorkBatch int
}
type ReportWorkDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
