package kvraft

import (
	"bytes"
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key       string
	Value     string
	Operation string // Get Put Append
	ClerkId   int64
	Seq       int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	logger       *Logger
	db           map[string]string
	clerkLastSeq map[int64]int64 // To check for duplicate requests
	notifyChans  map[int64]chan Op

	snapshotCh  chan bool
	lastApplied int
	persister   *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key:       args.Key,
		Operation: "Get",
		ClerkId:   args.ClerkId,
		Seq:       args.Seq,
	}

	kv.mu.Lock()
	// first check if this server is the leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d is not the leader for Get request on key %s", kv.me, args.Key))
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ch := make(chan Op, 1)
	kv.notifyChans[args.ClerkId] = ch
	kv.rf.Start(op)
	kv.mu.Unlock()

	kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d started a Get request for key %s from C%d", kv.me, args.Key, args.ClerkId))

	// set up a timer to avoid waiting indefinitely.
	timer := time.NewTimer(WaitTimeOut)
	defer timer.Stop()

	select {
	case <-timer.C: // timer expires
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d timed out waiting for Get request on key %s", kv.me, args.Key))
		reply.Err = ErrTimeOut
		return
	case resultOp := <-ch: // wait for the operation to be applied
		// check if the operation corresponds to the request
		if resultOp.ClerkId != args.ClerkId || resultOp.Seq != args.Seq {
			kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d received a non-matching Get result for key %s", kv.me, args.Key))
			reply.Err = ErrWrongLeader
		} else {
			kv.mu.Lock()
			reply.Value = kv.db[args.Key]
			reply.Err = OK
			kv.mu.Unlock()
			kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d successfully completed Get request for key %s", kv.me, args.Key))

		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Op, // Put Append
		ClerkId:   args.ClerkId,
		Seq:       args.Seq,
	}

	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d rejected PutAppend request on key %s as it's not the leader", kv.me, args.Key))
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ch := make(chan Op, 1)
	kv.notifyChans[args.ClerkId] = ch
	kv.rf.Start(op)
	kv.mu.Unlock()

	kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d started PutAppend (%s) request for key %s from C%d", kv.me, args.Op, args.Key, args.ClerkId))

	timer := time.NewTimer(WaitTimeOut)
	defer timer.Stop()

	select {
	case <-timer.C: // on time out
		kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d timed out on PutAppend request for key %s", kv.me, args.Key))
		reply.Err = ErrTimeOut
		return
	case resultOp := <-ch:
		// Check if the operation result corresponds to the original request.
		if resultOp.ClerkId != args.ClerkId || resultOp.Seq != args.Seq {
			kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d received non-matching result for PutAppend request on key %s", kv.me, args.Key))
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d successfully completed PutAppend request for key %s", kv.me, args.Key))

		}
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
	logger, err := NewLogger(1)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}

	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.logger = logger

	kv.db = make(map[string]string)
	kv.clerkLastSeq = make(map[int64]int64)
	kv.notifyChans = make(map[int64]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)

	kv.snapshotCh = make(chan bool)
	kv.persister = persister
	kv.readSnapshot(kv.persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()
	go kv.snapshotChecker()

	return kv
}

func (kv *KVServer) applier() {
	// continuously process messages from the applyCh channel
	for !kv.killed() {
		msg := <-kv.applyCh // wait for a message from Raft
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)

			// check if the operation is the latest from the clerk
			lastSeq, found := kv.clerkLastSeq[op.ClerkId]
			if !found || lastSeq < op.Seq {
				// apply the operation to the state machine
				kv.applyOperation(op)
				kv.clerkLastSeq[op.ClerkId] = op.Seq
			}

			// notify the waiting goroutine if it's present
			if ch, ok := kv.notifyChans[op.ClerkId]; ok {
				select {
				case ch <- op: // notify the waiting goroutine
					kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d notified the goroutine for ClerkId %d", kv.me, op.ClerkId))
				default:
					// if the channel is already full, skip to prevent blocking.

				}
			}

			kv.lastApplied = msg.CommandIndex
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d triggered a snapshot creation due to raft state size", kv.me))
				go func() { kv.snapshotCh <- true }()
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.readSnapshot(msg.Snapshot)
			kv.lastApplied = msg.SnapshotIndex
			kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d processed a snapshot message", kv.me))
		}
	}
}

func (kv *KVServer) applyOperation(op Op) {
	switch op.Operation {
	case "Put":
		kv.db[op.Key] = op.Value
	case "Append":
		kv.db[op.Key] += op.Value
	case "Get":
		// No state change for Get
	}
}

func (kv *KVServer) takeSnapshot() {
	kv.mu.Lock()

	// check if snapshot is necessary based on the maxraftstate size
	if kv.persister.RaftStateSize() <= kv.maxraftstate || kv.maxraftstate == -1 {
		kv.mu.Unlock()
		return
	}

	kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d starts taking a snapshot", kv.me))

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplied)
	e.Encode(kv.db)
	e.Encode(kv.clerkLastSeq)
	data := w.Bytes()
	index := kv.lastApplied
	kv.mu.Unlock()
	kv.rf.Snapshot(index, data)

	kv.logger.Log(LogTopicServer, fmt.Sprintf("S%d completed taking a snapshot", kv.me))

}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // empty snapshot
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var lastApplied int
	var db map[string]string
	var clerkLastSeq map[int64]int64

	if d.Decode(&lastApplied) != nil ||
		d.Decode(&db) != nil ||
		d.Decode(&clerkLastSeq) != nil {
		log.Fatal("readSnapshot: error decoding data")
	} else {
		kv.mu.Lock()
		kv.lastApplied = lastApplied
		kv.db = db
		kv.clerkLastSeq = clerkLastSeq
		kv.mu.Unlock()
	}
}

func (kv *KVServer) snapshotChecker() {
	for !kv.killed() {
		<-kv.snapshotCh
		kv.takeSnapshot()
	}
}
