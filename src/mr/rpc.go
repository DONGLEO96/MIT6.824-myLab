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
//worker call master for getting a task(map or reduce)
type GetTaskRequest struct {
	X int//暂时没用
}
type GetTaskResponse struct {
	MFileName string//map文件名字
	TaskName string//任务名字
	RFileName []string//reduce文件名字
	TaskType int//0:map,1:reduce,2:sleep
	ReduceNumber int
}
//worker send the message about outfile name to master
type ReportStatusRequest struct {
	FilesName []string//告诉master，中间文件的名字，reduce用不上
	TaskName string
	//FileName string//output file name
}
type ReportStatusResponse struct {
	X int
}
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
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
