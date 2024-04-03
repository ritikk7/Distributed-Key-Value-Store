package shardctrler

//
// Shardctrler clerk.
//

import (
	"cpsc416/labrpc"
	"crypto/rand"
	"fmt"
	"math/big"
	// "time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	// leaderId  int32 // Store the ID of the leader to try it first
	clientId  int64 // Unique client ID
	requestId int64 // Unique request ID for each request
	leaderId  int64
	logger    *Logger
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	logger, err := NewLogger(1)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.logger = logger
	// fmt.Println("made a clerk")
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.requestId++
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader && reply.Err != ErrTimeOut {
				return reply.Config
			}
		}
		// time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// fmt.Println("called a join")
	ck.logger.Log(LogTopicClerk, fmt.Sprintf("[JOIN] to server from %d", ck.clientId))
	ck.requestId++
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	for {
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if ok {
			if reply.Err == OK {
				return
			}
			if reply.WrongLeader {
				ck.cycleThroughLeaders()
			}
		} else {
			ck.cycleThroughLeaders()
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.requestId++
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId

	// try each known server.
	for {
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if ok {
			if reply.Err == OK {
				return
			}
			if reply.WrongLeader {
				ck.cycleThroughLeaders()
			}
		} else {
			ck.cycleThroughLeaders()
		}
	}
	// time.Sleep(100 * time.Millisecond)
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.requestId++
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err != ErrTimeOut {
				return
			}
		}
		// time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) cycleThroughLeaders() {
	ck.leaderId++
	ck.leaderId %= int64(len(ck.servers))
}
