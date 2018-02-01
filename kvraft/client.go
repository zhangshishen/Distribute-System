package raftkv

import (
	"crypto/rand"
	"fmt"
	"labrpc"
	"math/big"
	"strconv"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	// You will have to modify this struct.
	name        string
	frontLeader int
	index       int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.frontLeader = -1
	ck.index = 1
	// You'll have to add code here.
	ck.name = strconv.Itoa(int(nrand()))

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

//put and get will run synchronize ?
//
func (ck *Clerk) Get(key string) string {
	fmt.Printf("%s\t start to get %s\n", ck.name, key)
	args := GetArgs{Key: key, Name: ck.name}
	for {
		for i, k := range ck.servers {
			reply := GetReply{}
			ok := k.Call("KVServer.Get", &args, &reply)
			if !ok {
				continue
			}
			if reply.WrongLeader == false {
				ck.servers[0], ck.servers[i] = ck.servers[i], ck.servers[0]
				fmt.Printf("%s\t get value success %s\n", ck.name, reply.Value)
				return reply.Value
			} else {
				//fmt.Printf("%s\t get value failed \n", ck.name)
			}
		}
	}
	// You will have to modify this function.

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//ID from 1
	fmt.Printf("%s\t start to %s %s:%s\n", ck.name, op, key, value)
	args := PutAppendArgs{Key: key, Value: value, Op: op, Name: ck.name, ClientIndex: ck.index}

	for {
		for i, k := range ck.servers {
			reply := PutAppendReply{}
			//fmt.Printf("%s\t start to put %s\n", ck.name, key)
			ok := k.Call("KVServer.PutAppend", &args, &reply)
			//fmt.Printf("%s\t finished to put %s\n", ck.name, key)
			if !ok {
				continue
			}
			if reply.WrongLeader == false {
				fmt.Printf("%s\t %s %s:%s success\n", ck.name, op, key, value)
				ck.servers[0], ck.servers[i] = ck.servers[i], ck.servers[0]
				ck.index++
				return
			} else {
				//fmt.Printf("%s\t put %s failed,error = %s,reply = %d\n", ck.name, key, reply.Err, reply.WrongLeader)
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
