package kvraft

import (
	"cpsc416/labrpc"
	"crypto/rand"
	"fmt"
	"math/big"
)

type Clerk struct {
	servers        []*labrpc.ClientEnd
	clerkId        int64
	leaderId       int64
	seq 		   int64
	logger         *Logger
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:        servers,
		clerkId:        nrand(),
		seq: 			0,
	}

	var err error
	ck.logger, err = NewLogger(0)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}

	return ck
}

func (ck *Clerk) Get(key string) string {
	ck.seq++

	args := GetArgs{
		Key:            key,
		ClerkId:        ck.clerkId,
		Seq: ck.seq,
	}
	reply := GetReply{}

	for {
		ck.logger.Log(LogTopicClerk, fmt.Sprintf("[GET] Key %s to server", key))
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)

		if ok {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				ck.logger.Log(LogTopicClerk, fmt.Sprintf("Key %s does not exist in kvraft servers", key))
				return ""
			case ErrWrongLeader:
				ck.logger.Log(LogTopicClerk, fmt.Sprintf("Wrong Leader, try switching to a different leader..."))
				ck.cycleThroughLeaders()
			case ErrTimeOut:
				ck.logger.Log(LogTopicClerk, fmt.Sprintf("Time out for key %s", key))
			default:
				continue
			}
		} else {
			ck.cycleThroughLeaders()
		}
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seq++

	args := PutAppendArgs{
		ClerkId:        ck.clerkId,
		Seq: 			ck.seq,
		Key:            key,
		Value:          value,
		Op:             op,
	}
	reply := PutAppendReply{}

	for {
		ck.logger.Log(LogTopicClerk, fmt.Sprintf("[PutAppend] Key %s to server with op %s and value %s", key, op, value))
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader:
				ck.logger.Log(LogTopicClerk, fmt.Sprintf("Wrong Leader, try switching to a different leader..."))
				ck.cycleThroughLeaders()
			case ErrTimeOut:
				ck.logger.Log(LogTopicClerk, fmt.Sprintf("Time out for key %s", key))
			default:
				continue
			}
		} else {
			ck.cycleThroughLeaders()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) cycleThroughLeaders() {
	ck.leaderId++
	ck.leaderId %= int64(len(ck.servers))
}
