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
	//"fmt"
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

type ApplyMsg struct {
	Index        int
	CommandValid bool
	CommandIndex int
	Command      interface{}
	UseSnapshot  bool   // ignore for lab2; only used in lab3
	Snapshot     []byte // ignore for lab2; only used in lab3
}

type Log struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	term    int
	voteFor int

	log         []Log
	curHaveLog  int
	commitIndex int
	lastApplied int

	state      int
	matchIndex []int
	nextIndex  []int
	applyCh    chan ApplyMsg

	curElect          int
	receivedHeartbeat bool

	heartBreathChan    chan int
	requestForVoteChan chan int
	broadCast          chan int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

type RequestArg struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int

	Log          []Log
	LeaderCommit int
}

type ResponseArg struct {
	Success       bool
	Term          int
	CurMatchIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) AppendEntries(req *RequestArg, res *ResponseArg) {
	//fmt.Printf("%d received term %d entry from %d,term %d\n",rf.me,rf.term,req.LeaderID,req.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	res.Term = rf.term

	if req.Term < rf.term {
		res.Success = false
		res.Term = rf.term
		return
	}

	rf.receivedHeartbeat = true

	if req.Term > rf.term { // change leader
		rf.term = req.Term
		rf.voteFor = -1
		if rf.state == 4 { //change state
			rf.state = 2
			close(rf.heartBreathChan)
			rf.requestForVoteChan = make(chan int, 100)
			go rf.startWaitingHeartbeat()
		}
	}

	if rf.state != 2 {
		rf.state = 2
	}

	if req.PrevLogIndex > rf.curHaveLog { //log is not enough
		res.Success = false
		res.CurMatchIndex = rf.curHaveLog
	} else if rf.log[req.PrevLogIndex].Term == req.PrevLogTerm {
		//Append success
		res.Success = true
		copy(rf.log[req.PrevLogIndex+1:], req.Log)
		rf.curHaveLog = req.PrevLogIndex + len(req.Log)
		//fmt.Printf("cur have %d log,PrevLogIndex = %d\n",rf.curHaveLog,req.PrevLogIndex)
		if rf.curHaveLog < req.LeaderCommit {
			for i := rf.commitIndex + 1; i < rf.curHaveLog+1; i++ {
				rf.applyCh <- ApplyMsg{CommandIndex: i, Command: rf.log[i].Command, CommandValid: true}
			}
			rf.commitIndex = rf.curHaveLog
		} else {
			for i := rf.commitIndex + 1; i < req.LeaderCommit+1; i++ {
				rf.applyCh <- ApplyMsg{CommandIndex: i, Command: rf.log[i].Command, CommandValid: true}
			}
			rf.commitIndex = req.LeaderCommit
		}
	} else { //log is enough but not match
		for i := req.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term != rf.log[req.PrevLogIndex].Term {
				res.CurMatchIndex = i
				break
			}
		}
		res.Success = false
	}
	rf.persist()

}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.term
	isleader = (rf.state == 4)
	rf.mu.Unlock()
	return term, isleader

}

func (rf *Raft) waitForElection(i int) {

	rf.mu.Lock()
	curterm := rf.term
	if rf.state != 1 {
		rf.mu.Unlock()
		return
	}
	req := RequestVoteArgs{
		// Your data here (2A, 2B).
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: rf.curHaveLog,
		LastLogTerm:  rf.log[rf.curHaveLog].Term,
	}

	rf.mu.Unlock()
	res := RequestVoteReply{}
	ok := rf.peers[i].Call("Raft.RequestVote", &req, &res)

	if ok {

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.term == curterm && rf.state == 1 {
			if res.Success == true {
				rf.curElect++
				if rf.curElect+1 > len(rf.peers)/2 {
					//fmt.Printf("%d become leader\n",rf.me)
					close(rf.requestForVoteChan)
					rf.state = 4
					for i, _ := range rf.peers {
						rf.matchIndex[i] = 0
						rf.nextIndex[i] = rf.curHaveLog + 1
					}

					rf.heartBreathChan = make(chan int)
					go rf.startBreath()
					rf.broadCast <- 1
				}
			}

		} else {
			return
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = 1
	rf.term++
	rf.voteFor = rf.me
	rf.curElect = 0
	rf.mu.Unlock()
	//fmt.Printf("%d start election\n",rf.me)
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.waitForElection(i)
		}
	}
}

func (rf *Raft) startWaitingHeartbeat() {
	for {
		m := rand.Intn(150)
		m += 150

		select {
		case <-rf.requestForVoteChan:
			return
		case <-time.After(time.Duration(m) * time.Millisecond):
			if rf.receivedHeartbeat {
				rf.receivedHeartbeat = false
			} else {
				rf.startElection()
			}
		}
	}
}

func (rf *Raft) startBreath() {
	for {
		select {
		case <-rf.heartBreathChan:
			return
		case <-rf.broadCast:
			go rf.HeartBreath()
		case <-time.After(time.Duration(100) * time.Millisecond):
			go rf.HeartBreath()
		}
	}
}

func (rf *Raft) HeartBreath() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.SendEntriesTo(i)
		}
	}
}

func (rf *Raft) testCommit() {
	if rf.state != 4 {
		return
	}
	for {
		sum := 1
		for s, k := range rf.matchIndex {
			if s != rf.me && k > rf.commitIndex && rf.log[rf.matchIndex[s]].Term == rf.term {
				sum++
			}
		}
		if sum > len(rf.peers)/2 {
			rf.applyCh <- ApplyMsg{CommandIndex: rf.commitIndex + 1, Command: rf.log[rf.commitIndex+1].Command, CommandValid: true}
			rf.commitIndex++
		} else {
			return
		}
	}
}
func (rf *Raft) SendEntriesTo(i int) {

	rf.mu.Lock()
	curterm := rf.term
	if rf.state != 4 {
		rf.mu.Unlock()
		return
	}
	req := RequestArg{Term: rf.term, LeaderID: rf.me, PrevLogIndex: rf.nextIndex[i] - 1, PrevLogTerm: rf.log[rf.nextIndex[i]-1].Term, LeaderCommit: rf.commitIndex}
	if rf.nextIndex[i] <= rf.curHaveLog {
		req.Log = make([]Log, rf.curHaveLog-rf.nextIndex[i]+1)
		copy(req.Log, rf.log[rf.nextIndex[i]:rf.curHaveLog+1])
	} else {
		req.Log = make([]Log, 0)
	}

	rf.mu.Unlock()

	res := ResponseArg{}
	ok := rf.peers[i].Call("Raft.AppendEntries", &req, &res)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (!ok) || rf.term != curterm || (rf.nextIndex[i] != req.PrevLogIndex+1) {
		return
	}
	if res.Success { //test commit

		rf.nextIndex[i] += len(req.Log)
		rf.matchIndex[i] = rf.nextIndex[i] - 1

		if len(req.Log) != 0 {
			rf.testCommit()
			rf.persist()
		}

	} else {
		if curterm < res.Term || rf.term != curterm { //older term, turn to follower
		} else {
			rf.nextIndex[i] = res.CurMatchIndex + 1
		}

	}

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
	e := gob.NewEncoder(w)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.term)
	e.Encode(rf.curHaveLog)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
	d.Decode(&rf.term)
	d.Decode(&rf.curHaveLog)
	d.Decode(&rf.commitIndex)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term    int
	Success bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.receivedHeartbeat = true
	if args.Term < rf.term {

		reply.Term = rf.term
		reply.Success = false
		return
	}
	reply.Term = args.Term

	if rf.term < args.Term || (rf.term == args.Term && (rf.voteFor == -1 || rf.voteFor == args.CandidateId)) {
		if args.LastLogTerm > rf.log[rf.curHaveLog].Term {
			reply.Success = true
		} else if args.LastLogTerm == rf.log[rf.curHaveLog].Term && args.LastLogIndex >= rf.curHaveLog {
			reply.Success = true
		} else {
			reply.Success = false
		}
	} else { //same term and not vote for
		reply.Success = false
	}
	if rf.term < args.Term {
		if reply.Success == true {
			rf.term = args.Term
			rf.voteFor = args.CandidateId
		} else {
			rf.term = args.Term
			rf.voteFor = -1
		}
		if rf.state == 4 {
			close(rf.heartBreathChan)
			rf.requestForVoteChan = make(chan int, 100)
			go rf.startWaitingHeartbeat()
		}
		rf.state = 2
	} else { //equal term
		if rf.voteFor == -1 {
			rf.voteFor = args.CandidateId
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()
	index = rf.curHaveLog + 1
	term = rf.term
	isLeader = (rf.state == 4)

	if isLeader {
		rf.log[rf.curHaveLog+1].Command = command
		rf.log[rf.curHaveLog+1].Term = rf.term
		rf.curHaveLog++
		rf.persist()
		// Your code here (2B).
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	rf.broadCast = make(chan int, 100)
	rf.log = make([]Log, 10000)
	rf.commitIndex = 0
	rf.curHaveLog = 0
	rf.log[0].Term = -1
	rf.term = -1
	// Your initialization code here (2A, 2B, 2C).
	rf.voteFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.receivedHeartbeat = false
	rf.state = 2
	rf.applyCh = applyCh
	rf.curElect = 0

	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	rf.requestForVoteChan = make(chan int, 100)
	go rf.startWaitingHeartbeat()

	return rf
}
