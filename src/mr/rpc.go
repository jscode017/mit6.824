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

type ReqWorkArgs struct {
}
type ReqWorkReply struct {
	TaskType            string
	InputFileName       string
	ReduceID            int
	IntermediateFiles   []string
	NReduce             int
	SaltForIntermediate int
}

type WorkDoneArgs struct {
	TaskType              string
	IntermediateFileNames []string
	InputFileName         string
	OutPutID              int
}
type WorkDoneReply struct {
	//OutDated bool //if the task is already finished by other assigned workers
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
