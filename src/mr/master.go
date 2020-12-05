package mr

import (
	"log"
	"strconv"
	"strings"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

const (
	Map=iota
	Reduce
	Sleep
)
const (
	Working=iota
	Timeout
)
const (
	NotStarted=iota
	Processing
	Finished
)
type Task struct {
	Name string//任务名字
	Type int//任务类别
	Status int//任务状态，正常或者超时
	mFileName string//如果是map任务，则记录分配给该任务的文件名字
	rFileName int//如果是reduce任务，则记录分配给该任务的文件组编号
}
var taskNumber int = 0
type Master struct {
	// Your definitions here.
	mrecord map[string]int//记录需要map的文件，0表示未执行，1表示正在执行,2表示已经完成
	rrecord map[int]int//记录需要reduce的文件，0表示未执行，1表示正在执行,2表示已经完成
	reducefile map[int][]string//记录中间文件
	taskmap map[string]*Task //任务池，记录当前正在执行的任务
	mcount int//记录已经完成map的任务数量
	rcount int//记录已经完成的reduce的任务数量
	mapFinished bool//标志map任务是否已经完成
	reduceNumber int//需要执行的reduce的数量
	mutex sync.Mutex//锁
}
func (m *Master) HandleTimeout(taskName string){

	time.Sleep(time.Second*10)//睡眠十秒
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if t,ok:=m.taskmap[taskName];ok{//睡眠十秒这个任务还在执行，则意味着任务超时，需要处理
		t.Status=Timeout//任务设置为超时状态

		if t.Type==Map{
			f:=t.mFileName
			if m.mrecord[f]==Processing{//修改文件状态，还在执行中修改为未完成，方便分配给其他的worker
				m.mrecord[f]=NotStarted
			}
		}else if t.Type==Reduce{
			f:=t.rFileName
			if m.rrecord[f]==Processing{
				m.rrecord[f]=NotStarted
			}
		}
	}
}
func (m *Master) Report(args *ReportStatusRequest,reply* ReportStatusResponse)error{
	reply.X=1
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if t,ok:=m.taskmap[args.TaskName];ok{//如果还在任务池中
		flag:=t.Status
		if flag==Timeout{//如果任务已经超时了，忽略
			delete(m.taskmap, args.TaskName)
			return nil
		}
		ttype:=t.Type
		if ttype==Map{
			f:=t.mFileName
			m.mrecord[f]=Finished
			m.mcount+=1
			if m.mcount== len(m.mrecord){
				m.mapFinished=true
			}
			for _,v:=range args.FilesName{
				index:=strings.LastIndex(v,"_")
				num,err:=strconv.Atoi(v[index+1:])
				if err!=nil{
					log.Fatal(err)
				}
				m.reducefile[num] = append(m.reducefile[num], v)
			}
			delete(m.taskmap, t.Name)
			return nil
		}else if ttype==Reduce{
			rf:=t.rFileName
			m.rrecord[rf]=Finished
			m.rcount+=1
			delete(m.taskmap, t.Name)
			return nil
		}else{
			log.Fatal("task type is not map and reduce")
		}
	}
	log.Println("%s task is not in Master record",args.TaskName)
	return nil
}
func (m *Master) GetTask(args *GetTaskRequest,reply* GetTaskResponse) error{
	m.mutex.Lock()
	defer m.mutex.Unlock()
	reply.RFileName=make([]string,0)
	reply.ReduceNumber=m.reduceNumber
	reply.MFileName=""
	reply.TaskName=strconv.Itoa(taskNumber)
	taskNumber+=1
	if m.mapFinished{
		for v:=range m.rrecord{
			flag:=m.rrecord[v]
			if flag==Processing||flag==Finished{//如果这个任务正在执行或者已经结束，找下一个任务
				continue
			}else{
				m.rrecord[v]=Processing
				for _,filename:=range m.reducefile[v]{
					reply.RFileName = append(reply.RFileName, filename)
				}
				reply.TaskType=Reduce
				t:=&Task{reply.TaskName,reply.TaskType,Working,"",v}
				m.taskmap[reply.TaskName]=t
				go m.HandleTimeout(reply.TaskName)
				return nil
			}
		}
		reply.TaskType=Sleep
		return nil
	}else{
		//分配map任务
		for v,_:=range m.mrecord{
			flag:=m.mrecord[v]
			if flag==Processing||flag==Finished{//如果这个任务正在执行或者已经结束，找下一个任务
				continue
			}else{
				m.mrecord[v]=Processing//修改文件状态
				reply.MFileName=v
				reply.TaskType=Map
				t:=&Task{reply.TaskName,reply.TaskType,Working,reply.MFileName,-1}
				m.taskmap[reply.TaskName]=t

				go m.HandleTimeout(reply.TaskName)
				return nil
			}
		}

		reply.TaskType=Sleep
		return nil
	}
	return nil

}



// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("listen successed")
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.rcount==m.reduceNumber {
		ret = true
	}
	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mrecord: make(map[string]int),//记录需要map的文件，0表示未执行，1表示正在执行,2表示已经完成
		rrecord: make(map[int]int),//记录需要reduce的文件，0表示未执行，1表示正在执行,2表示已经完成
		reducefile: make(map[int][]string),//记录中间文件
		taskmap :make(map[string]*Task),
		mcount :0,//记录已经完成map的任务数量
		rcount :0,//记录已经完成的reduce的任务数量
		mapFinished:false,//
		reduceNumber :nReduce,//需要执行的reduce的数量
		mutex : sync.Mutex{},
	}
	//log.Println("MakeMaster")
	for _,f:=range files{
		m.mrecord[f]=0
	}
	for i:=0;i<nReduce;i++{
		m.rrecord[i]=0
	}
	// Your code here.


	m.server()
	return &m
}
