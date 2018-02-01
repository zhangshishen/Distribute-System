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
	state   int

	storage    map[string]string
	clientChan map[string]chan int

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
		if kv.state == LEADER && applyMsg.Term != kv.leaderTerm { //test if lose leadership
			close(kv.changeLeader)
			kv.state = FOLLOWER
			kv.clientChan[args.Name] = nil
		}

		if args.Op == "Get" {
			//fmt.Printf("Get success\n")
			if kv.clientChan[args.Name] != nil {
				//fmt.Printf("client %s:index %d get success %s:%s\n", args.Name, args.ClientIndex, args.Key, args.Value)
				go func(m chan int) { <-m }(kv.clientChan[args.Name])
			}

			kv.mu.Unlock()
			//fmt.Printf("%d\t finish received commit\n", kv.me)
			continue
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

				if kv.clientChan[args.Name] != nil { //notify receiver
					//fmt.Printf("client %s:index %d commit success %s:%s\n", args.Name, args.ClientIndex, args.Key, args.Value)
					go func(m chan int) { <-m }(kv.clientChan[args.Name])
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
	kv.clientChan[args.Name] = make(chan int)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		kv.clientChan[args.Name] = nil
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

	kv.mu.Lock()
	//fmt.Printf("start put append\n")
	reply.WrongLeader = true
	if (!isLeader) || (kv.leaderTerm != term) {
		//fmt.Printf("is not leader\n")
		if kv.state == LEADER {
			kv.state = FOLLOWER
			close(kv.changeLeader)
		}
		reply.WrongLeader = true
		reply.Err = "Is Not Leader"
		//fmt.Printf("finish put append,not leader\n")
		kv.mu.Unlock()
		return
	} else {
		// is leader (at least I think I am)
		//fmt.Printf("%s is leader\n", args.Name)

		if kv.state == FOLLOWER {
			kv.leaderTerm = term
			kv.state = LEADER
			kv.changeLeader = make(chan int)
		}
	}
	//fmt.Printf("finish put append\n")

	kv.mu.Unlock()
	//defer fmt.Printf("finish append %s:%s\n", args.Key, args.Name)
	for {
		select {
		case <-kv.changeLeader: //lose leadership
			reply.WrongLeader = true
			kv.mu.Lock()
			close(kv.clientChan[args.Name])
			kv.clientChan[args.Name] = nil
			kv.mu.Unlock()
			reply.Err = "Change Leader"
			fmt.Printf("lose leader\n")
			return
		case kv.clientChan[args.Name] <- 1: //normal commit
			//fmt.Printf("%d\t return success\n", kv.me)
			kv.mu.Lock()
			m, _ := strconv.Atoi(kv.storage[args.Name])
			if m >= args.ClientIndex {
				fmt.Printf("%d\t finish return %s\n", kv.me, args.Key)
				//defer fmt.Printf("%d\t has been return\n", kv.me)
				reply.WrongLeader = false
				reply.Err = "success"
				close(kv.clientChan[args.Name])
				kv.clientChan[args.Name] = nil
				kv.mu.Unlock()
				return
			} else {
				fmt.Printf("error\n")
				kv.mu.Unlock()
				//kv.clientChan[args.Name] = make(chan int, 1)
			}
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
	kv.clientChan = make(map[string](chan int))

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.ReceiveChan()
	// You may need initialization code here.

	return kv
}
