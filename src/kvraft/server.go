package kvraft

import (
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	entries         map[string]string
	lastClientCalls map[int64]int64
	clientReqs      map[int]chan Op
}

func (kv *KVServer) ReadApplyMessages() {
	for !kv.killed() {
		for msg := range kv.applyCh {
			kv.mu.Lock()

			if !msg.CommandValid {
				kv.mu.Unlock()
				continue
			}

			command := msg.Command.(Op)
			var opResult Op = command

			lastAppliedId, exists := kv.lastClientCalls[command.ClientId]
			if !exists || command.RequestId > lastAppliedId {
				kv.lastClientCalls[command.ClientId] = command.RequestId

				switch command.Type {
				case GET:
					value, ok := kv.entries[command.Key]
					if ok {
						opResult.Value = value
					}
				case PUT:
					kv.entries[command.Key] = command.Value
				case APPEND:
					kv.entries[command.Key] += command.Value
				}

				if ch, ok := kv.clientReqs[msg.CommandIndex]; ok {
					select {
					case ch <- opResult:
					default:
					}
				} else {
					kv.clientReqs[msg.CommandIndex] = make(chan Op, 1)
					kv.clientReqs[msg.CommandIndex] <- opResult
				}
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	lastAppliedId, ok := kv.lastClientCalls[args.ClientId]
	if ok && args.RequestId <= lastAppliedId {
		reply.Err = OK
		reply.Value = kv.entries[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{Type: GET, Key: args.Key, ClientId: args.ClientId, RequestId: args.RequestId}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch, ok := kv.clientReqs[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.clientReqs[index] = ch
	}
	kv.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp.ClientId == args.ClientId && appliedOp.RequestId == args.RequestId {
			reply.Err = OK
			reply.Value = appliedOp.Value
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(800 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	lastAppliedId, ok := kv.lastClientCalls[args.ClientId]
	if ok && args.RequestId <= lastAppliedId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	opType := PUT
	if args.Op == APPEND {
		opType = APPEND
	}
	op := Op{Type: opType, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch, ok := kv.clientReqs[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.clientReqs[index] = ch
	}
	kv.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp.ClientId == args.ClientId && appliedOp.RequestId == args.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(800 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.entries = make(map[string]string)
	kv.lastClientCalls = make(map[int64]int64)
	kv.clientReqs = make(map[int]chan Op)

	go kv.ReadApplyMessages()

	return kv
}
