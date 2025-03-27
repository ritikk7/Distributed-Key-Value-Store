package shardctrler

import (
	"cpsc416/labrpc"
	"crypto/rand"
	"fmt"
	"math/big"
)

// Clerk represents a client interacting with the ShardCtrler service.
type Clerk struct {
	servers   []*labrpc.ClientEnd // RPC endpoints of all servers
	clientId  int64               // Unique identifier for this client
	requestId int64               // Monotonic counter for requests
	leaderId  int64               // Best guess of current leader
	logger    *Logger             // Optional logger for debugging
}

// Generates a random 62-bit integer.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	randVal, _ := rand.Int(rand.Reader, max)
	return randVal.Int64()
}

// MakeClerk creates a new Clerk instance.
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	logger, err := NewLogger(1)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}
	return &Clerk{
		servers:   servers,
		clientId:  nrand(),
		requestId: 0,
		logger:    logger,
	}
}

// Query fetches the configuration with the given number (or latest if num == -1).
func (ck *Clerk) Query(num int) Config {
	ck.requestId++
	args := &QueryArgs{
		Num:       num,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	for {
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader && reply.Err != ErrTimeOut {
				return reply.Config
			}
		}
	}
}

// Join requests to add a new group of servers to the configuration.
func (ck *Clerk) Join(servers map[int][]string) {
	ck.logger.Log(LogTopicClerk, fmt.Sprintf("[JOIN] to server from %d", ck.clientId))
	ck.requestId++
	args := &JoinArgs{
		Servers:   servers,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	for {
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.Err == OK {
			return
		}
		ck.cycleThroughLeaders()
	}
}

// Leave requests to remove a set of groups from the configuration.
func (ck *Clerk) Leave(gids []int) {
	ck.requestId++
	args := &LeaveArgs{
		GIDs:      gids,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	for {
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.Err == OK {
			return
		}
		ck.cycleThroughLeaders()
	}
}

// Move assigns a specific shard to a particular group.
func (ck *Clerk) Move(shard int, gid int) {
	ck.requestId++
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	for {
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader && reply.Err != ErrTimeOut {
				return
			}
		}
	}
}

// cycleThroughLeaders moves to the next known server as the presumed leader.
func (ck *Clerk) cycleThroughLeaders() {
	ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
}
