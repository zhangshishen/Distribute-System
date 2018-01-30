package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
	"time"
)

const Debug = 0

const (
	FOLLOWER int = iota
	LEADER
)
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	state	int
	
	storage      map[string]string
	ClientChan   map[string]chan int

	maxraftstate int // snapshot if log grows this big
	leaderTerm   int // test if lose leadership

	notify    chan int
	changeLeader chan int
	isWaiting bool
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	putArg := PutAppendArgs{Op:"Get",Name:args.Name,ClientIndex:args.ClientIndex}
	kv.mu.Unlock()

	rep := GetReply{}
	kv.PutAppend(&putArg,&rep)

	if(rep.WrongLeader==false){
		reply.Value = storage[args.Key]
		reply.WrongLeader = false
	}else{
		reply.WrongLeader = true
	}

}

func (kv *KVServer) ReceiveChan() {
	for applyMsg := range kv.applyCh {

		
		kv.mu.Lock()
		if kv.state==LEADER && applyMsg.Term!=leaderTerm {	//test if lose leadership
			close(kv.changeLeader)
			kv.state = FOLLOWER
		}

		args := PutAppendArgs(applyMsg.Command)
		if args.Op == "Get" && kv.clientChan[args.Name] != nil{
			go func(m chan int){<-m}(kv.clientChan[args.Name])
			continue
		}
		kv.mu.Unlock()
		if kv.storage[args.Name] == "" {
			//first time a client commit
			kv.storage[args.Name] = "0"
		}
		if strconv.Atoi(kv.storage[args.Name]) >= args.ClientIndex {
			//have already apply
			continue
		} else {
			if strconv.Atoi(kv.storage[args.Name]) == args.ClientIndex-1 {

				kv.storage[args.Name] = strconv.Itoa(strconv.Atoi(kv.storage[args.Name]) + 1) //update index

				if args.Op == "Append" {
					kv.storage[args.Key] += args.Value
				} else if args.Op == "Put"{
					kv.storage[args.Key] = args.Value
				}
				kv.mu.Lock()
				if kv.clientChan[args.Name] != nil  { //notify receiver
					go func(m chan int){<-m}(kv.clientChan[args.Name])
				}
				kv.mu.Unlock()
			} else {
				fmt.Printf("error, raft server have holes\n")
			}
		}

	}
}
// what if a server has lost leadership and not realize ,so a client will always been block
//
// 
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// TODO
	if kv.clientChan[args.Name] != nil {
		fmt.Printf("already have chan\n")
	}
	

	index, term, isLeader := kv.rf.Start( /**/ )

	kv.mu.Lock()
	if (!isLeader)||(kv.leaderTerm!=term) {
		if(kv.state==LEADER){	
			kv.state=FOLLOWER
			close(kv.changeLeader)
		}
		reply.WrongLeader = true
		reply.Err = "Is Not Leader"
		kv.mu.Unlock()
		return
	}else {
		// is leader (at least I think I am)
		kv.clientChan[args.Name] = make(chan int)
		if(kv.state==FOLLOWER){
			kv.leaderTerm = term
			kv.state=LEADER
			kv.changeLeader = make(chan int)
		}
	}
	kv.mu.Unlock()

	for {
		select {
		case <-kv.changeLeader:				//lose leadership
			reply.WrongLeader = true
			reply.Err = "Is Not Leader"
			break
		case kv.clientChan[args.Name]<-1:	//normal commit
			if strconv.Atoi(kv.storage[args.Name]) >= args.ClientIndex {
				reply.WrongLeader = false
				reply.Err = nil
				close(kv.ClientChan[args.Name])
				kv.ClientChan[args.Name] = nil
				break
			} else {
				//kv.clientChan[args.Name] = make(chan int, 1)
			}
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.state = FOLLOWER
	// You may need initialization code here.

	kv.storage = make(map[string]string)
	kv.ClientChan = make(map[string](chan int))

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.ReceiveChan()
	// You may need initialization code here.

	return kv
}
