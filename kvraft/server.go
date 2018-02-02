package raftkv

import (
	//"fmt"
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

type Args struct {
	Term        int
	ClientIndex int
	Name        string
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
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	prevTerm int

	storage    map[string]string
	clientChan map[string](chan Args)

	maxraftstate int // snapshot if log grows this big
	leaderTerm   int // test if lose leadership

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

		kv.mu.Lock()
		args := applyMsg.Command.(Op)
		arg := Args{Term: applyMsg.Term, ClientIndex: args.ClientIndex, Name: args.Name}
		if kv.prevTerm < applyMsg.Term {
			kv.prevTerm = applyMsg.Term
			for _, v := range kv.clientChan {
				go func(a Args, va chan Args) { va <- a }(arg, v)
			}
		} else {
			if kv.clientChan[args.Name] != nil {
				go func(a Args, va chan Args) { va <- a }(arg, kv.clientChan[args.Name])
			}

		}

		if kv.storage[args.Name] == "" {
			//first time a client commit
			kv.storage[args.Name] = "0"
		}
		m, _ := strconv.Atoi(kv.storage[args.Name])
		kv.mu.Unlock()
		if m >= args.ClientIndex {
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

	reply.Err = "not been modified"

	kv.mu.Lock()
	kv.clientChan[args.Name] = make(chan Args)
	cchan := kv.clientChan[args.Name]
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if kv.clientChan != nil {
			delete(kv.clientChan, args.Name)
		}
		kv.mu.Unlock()
	}()
	op := Op{Key: args.Key,
		Value:       args.Value,
		Op:          args.Op,
		Name:        args.Name,
		ClientIndex: args.ClientIndex}

	_, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "Is Not Leader"
		return
	}

	for {
		//var result Args
		select {
		case result := <-cchan:
			if result.ClientIndex == args.ClientIndex && result.Name == args.Name {
				reply.WrongLeader = false
				return
			}
			if result.Term > term {
				reply.WrongLeader = true
				reply.Err = "Change Term"
				return
			}
		case <-time.After(time.Millisecond * 200):
			reply.WrongLeader = true
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
	kv.prevTerm = -1
	// You may need initialization code here.

	kv.storage = make(map[string]string)
	kv.clientChan = make(map[string](chan Args))

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.ReceiveChan()
	// You may need initialization code here.

	return kv
}
