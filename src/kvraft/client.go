package kvraft

import (
	"cpsc416/labrpc"
	"crypto/rand"
	"math/big"
	"sync/atomic"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int32 // Store the ID of the leader to try it first
	clientId  int64 // Unique client ID
	requestId int64 // Unique request ID for each request
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	reqId := atomic.AddInt64(&ck.requestId, 1) // Increment the request ID
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: reqId}

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (int(ck.leaderId) + i) % len(ck.servers)
			reply := GetReply{}
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				atomic.StoreInt32(&ck.leaderId, int32(server)) // Update the leader ID on successful response
				if reply.Err == OK {
					return reply.Value
				} else {
					return "" // Return empty string if key does not exist
				}
			}
		}
		// time.Sleep(100 * time.Millisecond) // Wait a bit before retrying
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	reqId := atomic.AddInt64(&ck.requestId, 1) // Increment the request ID
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, RequestId: reqId}

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (int(ck.leaderId) + i) % len(ck.servers)
			reply := PutAppendReply{}
			ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				atomic.StoreInt32(&ck.leaderId, int32(server)) // Update the leader ID on successful response
				return
			}
		}
		// time.Sleep(30 * time.Millisecond) // Wait a bit before retrying
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
