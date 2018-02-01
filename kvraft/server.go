package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
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

type Args struct {
	Term 	string
	ClientIndex	int
	Name string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name        string
	ClientIndex int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	prevTerm int

	storage    map[string]string
	clientChan map[string]chan Args

	maxraftstate int // snapshot if log grows this big
	leaderTerm   int // test if lose leadership

	notify       chan int
	changeLeader chan int
	isWaiting    bool
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	putArg := PutAppendArgs{Op: "Get", Name: args.Name, ClientIndex: args.ClientIndex}

	rep := PutAppendReply{}
	kv.PutAppend(&putArg, &rep)
	kv.mu.Lock()
	if rep.WrongLeader == false {
		reply.Value = kv.storage[args.Key]
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
	kv.mu.Unlock()

}

func (kv *KVServer) ReceiveChan() {
	for applyMsg := range kv.applyCh {
		//fmt.Printf("%d\t received commit\n", kv.me)
		kv.mu.Lock()

		args := applyMsg.Command.(Op)
		if kv.prevTerm < applyMsg.Term {
			kv.prevTerm = applyMsg.Term
			for k,v := kv.clientChan {
				arg := Args{Term:applyMsg.Term,ClientIndex:args.ClientIndex,Name:args.Name}
				v <- arg
			}
		}else {
			if kv.clientChan[args.Name]!=nil{
				kv.clientChan[args.Name] <- Args{Term:applyMsg.Term,ClientIndex:args.ClientIndex,Name:args.Name}
			}
			
		}

		if kv.storage[args.Name] == "" {
			//first time a client commit
			kv.storage[args.Name] = "0"
		}
		m, _ := strconv.Atoi(kv.storage[args.Name])
		kv.mu.Unlock()
		if m >= args.ClientIndex {
			//have already apply
			//fmt.Printf("%d\t finish received commit\n", kv.me)
			continue
		} else {
			if m == args.ClientIndex-1 {
				kv.mu.Lock()
				kv.storage[args.Name] = strconv.Itoa(m + 1) //update index
				if args.Op == "Append" {
					kv.storage[args.Key] += args.Value
				} else if args.Op == "Put" {
					kv.storage[args.Key] = args.Value
				}
				kv.mu.Unlock()
			} else {
				//fmt.Printf("error, raft server have holes\n")
			}
		}
		//fmt.Printf("%d\t finish received commit\n", kv.me)
	}
}

// what if a server has lost leadership and not realize ,so a client will always been block
//
//
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// TODO
	reply.Err = "not been modified"

	kv.mu.Lock()
	kv.clientChan[args.Name] = make(chan Args)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.clientChan,args.Name)
		kv.mu.Unlock()
	}()
	//fmt.Printf("start put append\n")
	//defer fmt.Printf("finish put append\n")
	op := Op{Key: args.Key,
		Value:       args.Value,
		Op:          args.Op,
		Name:        args.Name,
		ClientIndex: args.ClientIndex}

	_, term, isLeader := kv.rf.Start(op)
	
	
	//fmt.Printf("start put append\n")
	if !isLeader {
		//fmt.Printf("is not leader\n")
		reply.WrongLeader = true
		reply.Err = "Is Not Leader"
		//fmt.Printf("finish put append,not leader\n")
		
		return
	} 
	//fmt.Printf("finish put append\n")

	for {
		result <- kv.clientChan[args.Name]
		if(result.ClientIndex == args.ClientIndex && result.Name == args.Name ){
			reply.WrongLeader = false
			return
		}
		if(result.Term > term){
			reply.WrongLeader = true
			reply.Err = "Change Term"
			return
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
	kv.prevTerm = -1
	// You may need initialization code here.

	kv.storage = make(map[string]string)
	kv.clientChan = make(map[string](chan int))

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.ReceiveChan()
	// You may need initialization code here.

	return kv
}
