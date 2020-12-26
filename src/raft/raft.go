package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

 import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type Log struct {
	Term int
	IsHeartBeat bool
	IsEmpty bool
	Command interface{}
	LogIndex int
	//需要的其他参数以后再增加

}
const(
	leader=iota
	candidate
	follower
)
//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	losehbount int//配合定时器使用，每一个单位时间未收到心跳就自增，连续两个单位时间未收到心跳就开始投票

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state int//状态
	term int//任期，需要持久化
	votedfor int//当前任期已经投票给谁了，需要持久化
	log []Log//日志，需要持久化

	commitIndex int//需要持久化(理论上确实不需要持久化，因为重新上线的主机会根据主机的信息更改commit信号，不过持久化了更简单)，可以通过和leader通信恢复这个
	lastApplied int//暂时可以不持久化，每次节点重启之后重新执行所有log，但是持久化了会更好

	nextIndex []int//leader需要，记录给每个主机发的下一个log的index
	matchIndex []int//leader需要，记录已经复制给每个主机的最后一条log的index

	applyCh *chan ApplyMsg
	logCount int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term=rf.term
	isleader= rf.state==leader
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedfor)
	e.Encode(rf.log)
	//e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var xxx int
	var yyy int
	var zzz []Log
	//var aaa int
	if d.Decode(&xxx) != nil ||
	   d.Decode(&yyy) != nil ||d.Decode(&zzz)!=nil{//||
	//	d.Decode(&aaa)!=nil{
	  log.Fatal("readPersist error")
	} else {
		rf.mu.Lock()
		rf.term = xxx
	  	rf.votedfor = yyy
	  	rf.log=zzz
	  	//rf.commitIndex=aaa
		log.Printf("Peer %d(%d) :read persist data,log len is %d,commit is %d",rf.me,rf.term, len(rf.log),rf.commitIndex)
		rf.mu.Unlock()
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	Candidate int
	LastLogIndex int
	LastLogTerm int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int
	CommitIndex int
	Entry []Log
}
type AppendEntriesReply struct {
	Term int
	Success bool
	CommitIndex int
}
func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
func (rf *Raft) AppendEntries(args* AppendEntriesArgs,reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("Peer %d(%d) :%d(%d) request append",rf.me,rf.term,args.LeaderId,args.Term)

	if args.Term>=rf.term{
		//log.Printf("Peer %d(%d) :%d(%d) request append,i become follower",rf.me,rf.term,args.LeaderId,args.Term)
		rf.term=args.Term

		if rf.state!=follower{
			rf.BecomeFollowerWithLock(rf.term)
			rf.votedfor=args.LeaderId
		}
		rf.persist()
	}else {
		log.Printf("Peer %d(%d) :%d(%d) request append,term is less,can not append",rf.me,rf.term,args.LeaderId,args.Term)
		reply.Term=rf.term
		reply.Success=false
		reply.CommitIndex=rf.commitIndex
		return
	}
	if len(args.Entry)==0{
		//log.Printf("Peer %d(%d) :%d(%d) request append,is heart,append success",rf.me,rf.term,args.LeaderId,args.Term)
		if rf.commitIndex<args.CommitIndex{
			if !(args.PreLogIndex>= len(rf.log)||rf.log[args.PreLogIndex].Term!=args.PreLogTerm){
				rf.commitIndex=Min(args.PreLogIndex,args.CommitIndex)
			}
		}
		rf.losehbount=0
		reply.Success=true
		reply.CommitIndex=rf.commitIndex
		reply.Term=args.Term
	}else{
		if rf.commitIndex>=args.PreLogIndex+len(args.Entry){//发过来的所有日志已经在提交范围内了，没有意义了
			//log.Printf("Peer %d(%d) :%d(%d) %d commited,miss",rf.me,rf.term,args.LeaderId,args.Term,args.PreLogIndex+1)
			reply.Success=true
			reply.Term=rf.term
			reply.CommitIndex=rf.commitIndex
			return
		}
		if args.PreLogIndex>= len(rf.log)||rf.log[args.PreLogIndex].Term!=args.PreLogTerm{//日志太超前了，没有match上
			log.Printf("Peer %d(%d) log len is %d :%d request append index %d,match fail",rf.me,rf.term, len(rf.log),args.LeaderId,args.PreLogIndex+1)
			reply.Success=false
			reply.Term=args.Term
			reply.CommitIndex=rf.commitIndex
			return
		}else{//日志match上了

			log.Printf("Peer %d(%d) :%d(%d) request append,is Entry,match success",rf.me,rf.term,args.LeaderId,args.Term)
			rf.log=rf.log[0:args.PreLogIndex+1]
			for _,l:=range args.Entry{
				rf.log = append(rf.log, l)
			}
			//if len(rf.log)>args.PreLogIndex+len(args.Entry){//空间足够
			//
			//	//l:=Log{args.Entry.Term,args.Entry.IsHeartBeat,args.Entry.IsEmpty,args.Entry.Command,args.Entry.LogIndex}
			//	//rf.log[args.PreLogIndex+1]=l
			//	//rf.log=rf.log[0:args.PreLogIndex+1+1]//截断后续无效log
			//	//rf.persist()
			//}else if len(rf.log)==args.PreLogIndex+1{
			//	//l:=Log{args.Entry.Term,args.Entry.IsHeartBeat,args.Entry.IsEmpty,args.Entry.Command,args.Entry.LogIndex}
			//	//rf.log=append(rf.log, l)
			//	//rf.persist()
			//}else {
			//	log.Printf("Peer %d(%d) :%d(%d) request append,log number error",rf.me,rf.term,args.LeaderId,args.Term)
			//	log.Fatal()
			//}
			reply.Term=rf.term
			reply.Success=true
			if rf.commitIndex<args.CommitIndex{//commit的进度落后
				//如果args.CommitIndex>args.PreLogIndex+1
				//说明这条日志已经commit了，从机也该commit这条日志
				//如果如果args.CommitIndex<args.PreLogIndex+1
				//说明这条日志没有commit，但是之前的和从机都match上了，所以之前的可以commit了
				rf.commitIndex=Min(args.CommitIndex,Max(args.PreLogIndex,rf.commitIndex))
			}
			reply.CommitIndex=rf.commitIndex
			rf.persist()
			return
		}
	}

	return

}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("Peer %d(%d) :%d(%d) request vote",rf.me,rf.term,args.Candidate,args.Term)
	if args.Term<=rf.term{

		log.Printf("Peer %d(%d) :%d(%d) request vote,term is less my term,do not vote",rf.me,rf.term,args.Candidate,args.Term)
		reply.Term=args.Term
		reply.VoteGranted=false

		return
	}else {
		if args.LastLogTerm>rf.log[len(rf.log)-1].Term{
			log.Printf("Peer %d(%d) :%d(%d) request vote,log index is bigger,vote",rf.me,rf.term,args.Candidate,args.Term)
			rf.BecomeFollowerWithLock(args.Term)
			rf.votedfor=args.Candidate
			rf.persist()
			reply.Term=rf.term
			reply.VoteGranted=true
			return
		}else if args.LastLogTerm==rf.log[len(rf.log)-1].Term {
			if args.LastLogIndex>= len(rf.log){
				log.Printf("Peer %d(%d) :%d(%d) request vote,log term is bigger,vote",rf.me,rf.term,args.Candidate,args.Term)
				rf.BecomeFollowerWithLock(args.Term)
				rf.votedfor=args.Candidate
				rf.persist()
				reply.Term=rf.term
				reply.VoteGranted=true
				return
			}else{
				if rf.state==leader{//leader需要转换成为follower，启动定时器
					rf.BecomeFollowerWithLock(args.Term)
				}
				//follower和candidate不需要重启定时器，修改term就行了
				rf.term=args.Term
				rf.persist()
				log.Printf("Peer %d(%d) :%d(%d) request vote,log is not newer,do not vote",rf.me,rf.term,args.Candidate,args.Term)
				reply.Term=rf.term
				reply.VoteGranted=false
				return
			}
		}else{
			log.Printf("Peer %d(%d) :%d(%d) request vote,log is not newer,do not vote",rf.me,rf.term,args.Candidate,args.Term)
			if rf.state==leader{//leader需要转换成为follower，启动定时器
				rf.BecomeFollowerWithLock(args.Term)
			}
			//follower和candidate不需要重启定时器，修改term就行了
			rf.term=args.Term
			rf.persist()
			reply.Term=rf.term
			reply.VoteGranted=false
			return
		}
	}

	// Your code here (2A, 2B).
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int,args *AppendEntriesArgs,reply* AppendEntriesReply) bool{
	ok:=rf.peers[server].Call("Raft.AppendEntries",args,reply)
	return ok
}

func (rf *Raft) GetEmptyLogCountWithLock() int {
	i:=0
	for _,l:=range rf.log{
		if l.IsEmpty{
			i++
		}
	}
	return i
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state!=leader{
		//log.Printf("Peer %d(%d) :Start log,but is not leader",rf.me,rf.term)
		return -1,-1,false
	}
	//rf.logCount+=1
	index = len(rf.log)-rf.GetEmptyLogCountWithLock()+1
	logEntry := Log{
		Command: command,
		Term:    rf.term,
		IsHeartBeat: false,
		IsEmpty: false,
		LogIndex: index,
	}
	rf.log = append(rf.log, logEntry)


	term=rf.term
	rf.persist()
	//log.Printf("Peer %d(%d) :Leader Start log index:%d,term:%d",rf.me,rf.term,index,term)
	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	//log.Printf("Peer %d(%d) :Killed",rf.me,rf.term)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) BecomeFollowerWithLock(term int){
	log.Printf("Peer %d(%d) :become follower",rf.me,rf.term)
	rf.losehbount=0
	rf.term=term
	rf.state=follower
	rf.votedfor=-1
}
func (rf *Raft) BecomeLeaderWithLock(){
	rf.votedfor=-1
	rf.state=leader
	log.Printf("Peer %d(%d) :become leader",rf.me,rf.term)
	rf.losehbount=0
	for i:=0;i< len(rf.matchIndex);i++{
		rf.nextIndex[i]= len(rf.log)//将要发送的是下一条log
		rf.matchIndex[i]=0
	}

	//初始化了应该马上发一条空log用于主从同步
	logEntry := Log{
		Command: -1,
		Term:    rf.term,
		IsHeartBeat: false,
		IsEmpty: true,
		LogIndex: 0,
	}
	rf.log = append(rf.log, logEntry)

	rf.persist()

	// Your code here (2B).
	for i:=0;i< len(rf.peers);i++{
		if i==rf.me{
			continue
		}
		curr:=i
		go rf.SendAppendEntriesFunc(curr)
	}

}
func (rf *Raft) BecomeCandidate(){
	log.Printf("Peer %d(%d,%d) :become candidate",rf.me,rf.term,rf.state)
	for{
		t:=rand.Intn(100)
		time.Sleep(time.Millisecond*time.Duration(t))
		if rf.killed(){
			return
		}
		rf.mu.Lock()
		//defer rf.mu.Unlock()
		if rf.state==follower||rf.state==leader{//如果在休眠这段时间，状态已经转变了，直接返回（可能被其他主机抢先了）
			rf.mu.Unlock()
			return
		}
		//是候选者状态
		rf.term+=1//任期自增
		rf.votedfor=rf.me//投票给自己
		rf.persist()//持久化状态
		log.Printf("Peer %d(%d,%d) :start vote request",rf.me,rf.term,rf.state)
		rf.mu.Unlock()
		var count int32 =1//投票计数器，首先投给自己
		var w = sync.WaitGroup{}

		for i:=0;i< len(rf.peers);i++{//想起他几个节点发起投票请求
			if rf.me==i{
				continue
			}
			w.Add(1)
			args:=&RequestVoteArgs{rf.term,rf.me, len(rf.log),rf.log[len(rf.log)-1].Term}
			reply:=&RequestVoteReply{0,false}
			curr:=i
			go func() {
				ok:=rf.sendRequestVote(curr,args,reply)
				w.Done()
				//log.Printf("Peer %d(%d) :%d(%d) request vote,res:%t",rf.me,rf.term,curr,reply.Term,ok)
				if ok {//rpc成功,处理回复
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.VoteGranted{//如果投票给我了
						atomic.AddInt32(&count,1)
					}else{//如果没投给我
						if reply.Term==rf.term{
							//任期和我一致，但是不投票给我，说明投给别的候选人了，不处理

						}else if reply.Term>rf.term{
							//任期比我大，别的候选人更快，或者已经有新的leader了，变回follower状态
							rf.BecomeFollowerWithLock(reply.Term)
							//rf.mu.Unlock()
							return
						}else{
							//任期比我小但是不投票给我，可能是超时回来的投票
							//log.Printf("Peer %d(%d) :%d(%d) do not vote me",rf.me,rf.term,curr,reply.Term)
							//log.Fatal()
							//log.Fatal()
						}
					}
				}
			}()
			//如果出问题了，不用管
		}
		var ch = make(chan bool)

		go func() {
			w.Wait()
			ch <- false
		}()
		select {
		case <-time.After(time.Millisecond*100):
			rf.mu.Lock()
			log.Printf("Peer %d(%d,%d) : vote rpc timeout",rf.me,rf.term,rf.state)
			rf.mu.Unlock()
		case <-ch:
			rf.mu.Lock()
			log.Printf("Peer %d(%d,%d) : vote rpc finished",rf.me,rf.term,rf.state)
			rf.mu.Unlock()
		}



		rf.mu.Lock()
		if rf.state!=candidate{//可能已经重新变回follower了
			rf.mu.Unlock()
			return
		}
		log.Printf("Peer %d(%d,%d) :vote count %d",rf.me,rf.term,rf.state,atomic.LoadInt32(&count))

		if atomic.LoadInt32(&count)*2> int32(len(rf.peers)){//如果投票过半，升任主机
			rf.BecomeLeaderWithLock()
			rf.mu.Unlock()
			return
		}
		rf.BecomeFollowerWithLock(rf.term)//恢复成为follower，等待下一次超时
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) HeartBeat(){
	for{
		//log.Printf("Peer %d(%d,%d) :heartbeat",rf.me,rf.term,rf.state)
		time.Sleep(time.Millisecond*100)
		rf.mu.Lock()
		if rf.state==leader{//只有leader需要发送心跳
			l:=make([]Log,0)


			for i:=0;i< len(rf.peers);i++{
				if i==rf.me{
					continue
				}
				args:=&AppendEntriesArgs{rf.term,rf.me, rf.nextIndex[i]-1,rf.log[rf.nextIndex[i]-1].Term,rf.commitIndex,l}
				reply:=&AppendEntriesReply{0,false,0}
				curr:=i
				//log.Printf("Peer %d(%d) :%d send heartbeat",rf.me,rf.term,curr)
				go func() {
					rf.sendAppendEntries(curr,args,reply)
				}()
			}
			//log.Printf("Peer %d(%d) :send all heartbeat",rf.me,rf.term)
		}
		rf.mu.Unlock()
		if rf.killed(){
			return
		}
	}
}
func (rf *Raft) BackWork(){
	for{
		time.Sleep(time.Millisecond*50)
		if rf.killed(){
			return
		}
		rf.mu.Lock()
		//log.Print("matchIndex",rf.matchIndex)
		mi:=make([]int, len(rf.matchIndex))
		copy(mi,rf.matchIndex)
		mi[rf.me]= len(rf.log)-1
		sort.Ints(mi)
		if rf.state==leader{
			midIndex:= len(mi)/2
			if rf.log[mi[midIndex]].Term==rf.term{//不负责为之前任期的leader留下的过半复制log专门进行提交，只能提交自己任期内的log
				//提交自己任期log时能够自动把之前的都提交了
				//paper Figure 8
				rf.commitIndex=Max(mi[midIndex],rf.commitIndex)
			}

		}
		rf.persist()
		for ;rf.lastApplied<=rf.commitIndex;rf.lastApplied++{
			log.Printf("Peer %d(%d) :%d apply",rf.me,rf.term,rf.lastApplied)
			if rf.log[rf.lastApplied].IsEmpty{//非用户指令，不用apply
				continue
			}else{
				//log.Printf("Peer %d(%d) :apply %d(%d)",rf.me,rf.term,rf.log[rf.lastApplied].LogIndex,rf.log[rf.lastApplied].Command.(int))
				m:=ApplyMsg{true,rf.log[rf.lastApplied].Command,rf.log[rf.lastApplied].LogIndex 	}
				*rf.applyCh <- m
			}
			//apply
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) Timer(){//处理超时未收到心跳的问题
	for{
		time.Sleep(time.Millisecond*100)
		if rf.killed(){
			return
		}
		rf.mu.Lock()

		if rf.state==leader{//leader不需要定时器
			rf.mu.Unlock()
			continue
		}else if rf.state==candidate{//不需要定时器
			rf.mu.Unlock()
			continue
		}else if rf.state==follower{
			rf.losehbount+=1//心跳超时次数加1
			//log.Printf("Peer %d(%d,%d) :tick %d",rf.me,rf.term,rf.state,rf.losehbount);
			if rf.losehbount==3{//超时两次，开始状态转换

				rf.losehbount=0//计数器置零
				rf.state=candidate
				go rf.BecomeCandidate()
			}
			rf.mu.Unlock()
		}

	}
}
func (rf *Raft) LogThread() {
	for{
		time.Sleep(100*time.Millisecond)
		if rf.killed(){
			return
		}
		rf.mu.Lock()
		s:=""
		s+=strconv.Itoa(rf.commitIndex)
		s+="["
		for _,l:=range rf.log{
			s+=strconv.Itoa(l.Term)

				s+="("
			switch l.Command.(type) {
			case int:
				s+=strconv.Itoa(l.Command.(int))
			case string:
				s+=l.Command.(string)
			default:
				log.Printf("log type error")
				log.Fatal()
			}

				s+=")"

			s+=","
		}
		s+="]"
		log.Print(rf.me,":",s)
		rf.mu.Unlock()
	}
}
func (rf *Raft) SendAppendEntriesFunc(peer int){
	for{
		time.Sleep(time.Millisecond*50)//50ms检测一次是否同步完成
		if rf.killed(){
			return
		}
		for {
			rf.mu.Lock()
			if rf.state!=leader{//不是leader了，不用发了
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[peer]>= len(rf.log){//同步完成
				rf.mu.Unlock()
				break//跳出循环
			}
			log.Printf("Peer %d(%d) :%d send log index %d-%d",rf.me,rf.term,peer,rf.nextIndex[peer], len(rf.log))
			var data=make([]Log, len(rf.log[rf.nextIndex[peer]:len(rf.log)]))
			copy(data,rf.log[rf.nextIndex[peer]:len(rf.log)])
			log.Println("data len is", len(data))
			args:=&AppendEntriesArgs{rf.term,rf.me, rf.nextIndex[peer]-1,rf.log[rf.nextIndex[peer]-1].Term,rf.commitIndex,data}
			reply:=&AppendEntriesReply{0,false,0}
			rf.mu.Unlock()
			w:=sync.WaitGroup{}
			w.Add(1)
			ok:=false
			go func() {
				ok=rf.sendAppendEntries(peer,args,reply)
				w.Done()
			}()
			var ch = make(chan bool)
			go func() {
				w.Wait()
				ch <- false
			}()
			select {
			case <-time.After(time.Millisecond*100):
				rf.mu.Lock()
				log.Printf("Peer %d(%d,%d) : append log to %d timeout",rf.me,rf.term,rf.state,peer)
				rf.mu.Unlock()
			case <-ch:
				rf.mu.Lock()
				log.Printf("Peer %d(%d,%d) : append log to %d finished",rf.me,rf.term,rf.state,peer)
				rf.mu.Unlock()
			}
			if ok{
				if reply.Success{//添加成功了

					rf.mu.Lock()
					//log.Printf("Peer %d(%d) :modify %d matchindex %d to %d",rf.me,rf.term,peer,rf.matchIndex[peer],Max(rf.matchIndex[peer],rf.nextIndex[peer]))
					rf.matchIndex[peer]=Max(rf.matchIndex[peer],rf.nextIndex[peer]+len(data)-1)
					rf.nextIndex[peer]+= len(data)
					log.Printf("Peer %d(%d) :%d send log index %d(%d) Success,nextindex is %d",rf.me,rf.term,peer,rf.nextIndex[peer], len(data),rf.nextIndex[peer])
					rf.mu.Unlock()
				}else{//添加失败了
					rf.mu.Lock()
					if reply.Term<=rf.term{//term比我小但是添加失败，是因为没有match，调整nextindex
						log.Printf("Peer %d(%d) :%d send log index %d Fail,match fail",rf.me,rf.term,peer,rf.nextIndex[peer])
						rf.nextIndex[peer]=reply.CommitIndex+1//从commit的下一个log开始发
						rf.mu.Unlock()
					}else{//否则是term比我大，不用给他发数据了，等着重新选主
						rf.mu.Unlock()
						return
					}

				}
			}else{
				break
			}

		}
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.losehbount=0
	rf.state=follower
	rf.term=1
	rf.votedfor=-1
	rf.log=make([]Log,0)
	rf.commitIndex=0
	rf.lastApplied=0
	rf.nextIndex=make([]int, len(peers))
	rf.matchIndex=make([]int, len(peers))
	rf.applyCh=&applyCh
	rf.logCount=0
	rand.Seed(time.Now().UnixNano())
	log.Printf("peer %d Start...",me)
	for i:=0;i< len(rf.nextIndex);i++{
		rf.nextIndex[i]=0
		rf.matchIndex[i]=0
	}
	l:=Log{0,false,true,-1,0}
	rf.log=append(rf.log,l)

	//time.Sleep(time.Millisecond*1000)//等待上一次的进程全部退出
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Timer()//启动定时器
	go rf.HeartBeat()//启动心跳发送
	go rf.BackWork()//后台回收线程
	go rf.LogThread()
	return rf
}
