package kvraft

import (
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"log"
	"sync"
	"sync/atomic"
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
	Op    string
	Key   string
	Value string
	Term  int
	Err   Err
	Index int
}

type Entry struct {
	key   string
	value string
}
type ClientInfo struct {
	lastReqID int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	entries         []Entry
	clientReqs      map[int]chan Op
	lastClientCalls map[int64]ClientInfo
}

func (kv *KVServer) ReadApplyMessages() {
	for !kv.killed() {
		for msg := range kv.applyCh {

			if msg.CommandValid {
				command := msg.Command.(Op)
				command.Index = msg.CommandIndex
				if command.Op == GET {
					kv.mu.Lock()
					command.Err = ErrNoKey
					command.Value = ""
					for _, entry := range kv.entries {
						if entry.key == command.Key {
							command.Err = OK
							command.Value = entry.value
							break
						}
					}
					kv.mu.Unlock()
					// apply nothing in particular, just let the client know it worked
				} else if command.Op == PUT || command.Op == APPEND {
					hasKey := false
					kv.mu.Lock()
					for i, entry := range kv.entries {
						if entry.key == command.Key {
							hasKey = true
							if command.Op == PUT {
								// put command
								kv.entries[i].value = command.Value
							} else {
								// append command
								kv.entries[i].value = entry.value + command.Value
							}
							break
						}
					}
					// otherwise if no key was found
					if !hasKey {
						kv.entries = append(kv.entries, Entry{key: command.Key, value: command.Value})
					}
					kv.mu.Unlock()
				}
				kv.clientReqs[msg.CommandIndex] <- command
			}
		}

	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	go func() {
		if kv.killed() {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		_, ok := kv.lastClientCalls[args.ClientId]
		if ok {
			if kv.lastClientCalls[args.ClientId].lastReqID >= args.RequestId {
				reply.Err = ErrOldRequest
				kv.mu.Unlock()
				return
			}
		}

		kv.lastClientCalls[args.ClientId] = ClientInfo{args.RequestId}
		kv.mu.Unlock()

		op := Op{Op: GET, Key: args.Key}
		index, _, isLeader := kv.rf.Start(op)

		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		result := <-kv.clientReqs[index]

		if result.Op != GET || result.Key != args.Key {
			reply.Err = ErrWrongLeader
			return
		}

		if result.Err == ErrNoKey {
			reply.Err = ErrNoKey
			reply.Value = ""
			return
		}
		reply.Err = OK
		reply.Value = result.Value
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// check if the current raft is killed or is the leader and submit operation through Start()

	go func() {
		if kv.killed() {
			reply.Err = ErrWrongLeader
			return
		}

		kv.mu.Lock()
		_, ok := kv.lastClientCalls[args.ClientId]
		if ok {
			if kv.lastClientCalls[args.ClientId].lastReqID >= args.RequestId {
				reply.Err = ErrOldRequest
				kv.mu.Unlock()
				return
			}
		}

		kv.lastClientCalls[args.ClientId] = ClientInfo{args.RequestId}
		kv.mu.Unlock()

		op := Op{Op: args.Op, Key: args.Key, Value: args.Value}
		index, _, isLeader := kv.rf.Start(op)

		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		result := <-kv.clientReqs[index]
		if !(result.Op == APPEND || result.Op == PUT) {
			reply.Err = ErrWrongLeader
			return
		}

		if result.Key != args.Key {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
	}()
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
	kv.entries = make([]Entry, 0)
	kv.clientReqs = make(map[int]chan Op)
	kv.lastClientCalls = make(map[int64]ClientInfo)
	// kv.entries[0] = Entry{key: "0", value: "x 0 0 y"}

	go kv.ReadApplyMessages()

	return kv
}
