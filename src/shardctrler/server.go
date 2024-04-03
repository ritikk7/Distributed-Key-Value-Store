package shardctrler

import (
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	logger          *Logger
	dead            int32 // set by Kill()
	lastClientCalls map[int64]int64
	clientReqs      map[int64]chan Op
	lastConfigNum   int
	configs         []Config // indexed by config num
	lastApplied     int
}

type Op struct {
	// Your data here.
	Type      string
	ClientId  int64
	RequestId int64
	// join
	Servers map[int][]string
	// leave
	GIDs []int
	// move
	Shard int
	GID   int
	// query
	Num int
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) ReadApplyMessages() {
	for !sc.killed() {
		msg := <-sc.applyCh // wait for a message from Raft
		if msg.CommandValid {
			sc.mu.Lock()
			if msg.CommandIndex <= sc.lastApplied {
				sc.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)

			// check if the operation is the latest from the clerk
			lastSeq, found := sc.lastClientCalls[op.ClientId]
			if !found || lastSeq < op.RequestId {
				// apply the operation to the state machine
				sc.applyOperation(op)
				sc.lastClientCalls[op.RequestId] = op.RequestId
			}

			// notify the waiting goroutine if it's present
			if ch, ok := sc.clientReqs[op.ClientId]; ok {
				select {
				case ch <- op: // notify the waiting goroutine
					// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d notified the goroutine for ClerkId %d", sc.me, op.ClerkId))
				default:
					// if the channel is already full, skip to prevent blocking.

				}
			}

			sc.lastApplied = msg.CommandIndex
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		Type:      JOIN, // Put Append
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Servers:   args.Servers,
	}

	sc.mu.Lock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d rejected Join request as it's not the leader", sc.me))
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ch := make(chan Op, 1)
	sc.clientReqs[args.ClientId] = ch
	sc.rf.Start(op)
	sc.mu.Unlock()

	sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d started Join from C%d", sc.me, args.ClientId))

	timer := time.NewTimer(WaitTimeOut)
	defer timer.Stop()

	select {
	case <-timer.C: // on time out
		sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d timed out on Join request", sc.me))
		reply.Err = ErrTimeOut
		return
	case resultOp := <-ch:
		// Check if the operation result corresponds to the original request.
		if resultOp.ClientId != args.ClientId || resultOp.RequestId != args.RequestId {
			sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d received non-matching result for Join request", sc.me))
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d successfully completed Join request", sc.me))

		}
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Type:      LEAVE, // Put Append
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		GIDs:      args.GIDs,
	}

	sc.mu.Lock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d rejected PutAppend request on key %s as it's not the leader", sc.me, args.Key))
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ch := make(chan Op, 1)
	sc.clientReqs[args.ClientId] = ch
	sc.rf.Start(op)
	sc.mu.Unlock()

	// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d started PutAppend (%s) request for key %s from C%d", sc.me, args.Op, args.Key, args.ClerkId))

	timer := time.NewTimer(WaitTimeOut)
	defer timer.Stop()

	select {
	case <-timer.C: // on time out
		// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d timed out on PutAppend request for key %s", sc.me, args.Key))
		reply.Err = ErrTimeOut
		return
	case resultOp := <-ch:
		// Check if the operation result corresponds to the original request.
		if resultOp.ClientId != args.ClientId || resultOp.RequestId != args.RequestId {
			// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d received non-matching result for PutAppend request on key %s", sc.me, args.Key))
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d successfully completed PutAppend request for key %s", sc.me, args.Key))

		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:      MOVE, // Put Append
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	sc.mu.Lock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d rejected PutAppend request on key %s as it's not the leader", sc.me, args.Key))
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ch := make(chan Op, 1)
	sc.clientReqs[args.ClientId] = ch
	sc.rf.Start(op)
	sc.mu.Unlock()

	// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d started PutAppend (%s) request for key %s from C%d", sc.me, args.Op, args.Key, args.ClerkId))

	timer := time.NewTimer(WaitTimeOut)
	defer timer.Stop()

	select {
	case <-timer.C: // on time out
		// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d timed out on PutAppend request for key %s", sc.me, args.Key))
		reply.Err = ErrTimeOut
		return
	case resultOp := <-ch:
		// Check if the operation result corresponds to the original request.
		if resultOp.ClientId != args.ClientId || resultOp.RequestId != args.RequestId {
			// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d received non-matching result for PutAppend request on key %s", sc.me, args.Key))
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d successfully completed PutAppend request for key %s", sc.me, args.Key))

		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type:      QUERY, // Put Append
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Num:       args.Num,
	}

	sc.mu.Lock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d rejected PutAppend request on key %s as it's not the leader", sc.me, args.Key))
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ch := make(chan Op, 1)
	sc.clientReqs[args.ClientId] = ch
	sc.rf.Start(op)
	sc.mu.Unlock()

	// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d started PutAppend (%s) request for key %s from C%d", sc.me, args.Op, args.Key, args.ClerkId))

	timer := time.NewTimer(WaitTimeOut)
	defer timer.Stop()

	select {
	case <-timer.C: // on time out
		// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d timed out on PutAppend request for key %s", sc.me, args.Key))
		reply.Err = ErrTimeOut
		return
	case resultOp := <-ch:
		// Check if the operation result corresponds to the original request.
		if resultOp.ClientId != args.ClientId || resultOp.RequestId != args.RequestId {
			// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d received non-matching result for PutAppend request on key %s", sc.me, args.Key))
			reply.WrongLeader = true
		} else {
			sc.mu.Lock()
			if args.Num >= len(sc.configs) || args.Num == -1 {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[args.Num]
			}
			reply.Err = OK
			sc.mu.Unlock()
			// sc.logger.Log(LogTopicServer, fmt.Sprintf("S%d successfully completed PutAppend request for key %s", sc.me, args.Key))

		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	logger, err := NewLogger(1)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.logger = logger

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs[0].Num = 0
	sc.configs[0].Groups = make(map[int][]string)
	// sc.configs[0].Shards = make([]int, NShards)
	sc.lastClientCalls = make(map[int64]int64)
	sc.clientReqs = make(map[int64]chan Op)

	go sc.ReadApplyMessages()
	return sc
}

func (sc *ShardCtrler) applyOperation(opResult Op) {
	switch opResult.Type {
	case JOIN:
		// make a new configuration with new replica groups
		oldGIDs := sc.configs[sc.lastConfigNum].Groups
		newGIDs := make(map[int][]string)
		numGroups := 0

		// copy old groups (that have not been removed) into new group
		for gid, servers := range oldGIDs {
			if len(servers) != 0 {
				numGroups++
				newGIDs[gid] = servers
			}
		}
		newNumGroups := numGroups
		for gid, servers := range opResult.Servers {
			val, ok := oldGIDs[gid]
			if !ok || len(val) == 0 {
				// that means that this has not been assigned before or is currently not in use
				newNumGroups++
				newGIDs[gid] = servers
			}
		}
		// divide shards evenly among all groups with as little movement as possible (move shards from prev groups to new groups)
		var newShards [NShards]int
		newShardsPerGroup := NShards / newNumGroups
		oldShardsPerGroup := 0
		if numGroups != 0 {
			oldShardsPerGroup = NShards / numGroups
		}

		shardsToBeMovedPerOldGroup := oldShardsPerGroup - newShardsPerGroup
		counts := make(map[int]int)
		counts_new := make(map[int]int)
		for i := 0; i < NShards; i++ {
			currOld := sc.configs[sc.lastConfigNum].Shards[i]
			_, ok := counts[currOld]
			if !ok {
				counts[currOld] = 0
			}
			val := counts[currOld]
			if val < shardsToBeMovedPerOldGroup {
				counts[currOld]++
				for currNew, _ := range opResult.Servers {
					_, hasCount := counts_new[currNew]
					if !hasCount {
						counts_new[currNew] = 0
					}
					count := counts_new[currNew]
					if count < shardsToBeMovedPerOldGroup {
						counts_new[currNew]++
						newShards[i] = currNew
						break
					}
				}
			} else {
				newShards[i] = currOld
			}

		}
		newConfigNum := sc.lastConfigNum + 1
		//now assign the new config
		newConfig := Config{Num: newConfigNum, Shards: newShards, Groups: newGIDs}
		sc.configs = append(sc.configs, newConfig)
		sc.lastConfigNum++

	case LEAVE:
		// make a new configuration without given replica groups
		oldGIDs := sc.configs[sc.lastConfigNum].Groups
		newGIDs := make(map[int][]string)
		numGroups := 0
		newNumGroups := 0
		// copy old groups (that will not be removed)
		for gid, servers := range oldGIDs {
			if len(servers) != 0 {
				toBeRemoved := false
				numGroups++
				for _, rGid := range opResult.GIDs {
					if gid == rGid {
						toBeRemoved = true
						break
					}
				}
				if !toBeRemoved && len(servers) > 0 {
					newGIDs[gid] = servers
					newNumGroups++
				}
			}
		}
		// divides shards evenly among the remaining groups with as few moves as possible
		var newShards [NShards]int
		if newNumGroups == 0 {
			newConfigNum := sc.lastConfigNum + 1
			//now assign the new config
			newConfig := Config{Num: newConfigNum, Shards: newShards, Groups: newGIDs}
			sc.configs = append(sc.configs, newConfig)
			sc.lastConfigNum++
			break
		}
		newShardsPerGroup := int(math.Ceil(float64(NShards) / float64(newNumGroups)))
		oldShardsPerGroup := NShards / numGroups
		shardsToBeAddedPerGroup := newShardsPerGroup - oldShardsPerGroup
		counts_remaining := make(map[int]int)
		for i := 0; i < NShards; i++ {
			currOld := sc.configs[sc.lastConfigNum].Shards[i]
			kept := true
			for _, rGid := range opResult.GIDs {
				if currOld == rGid {
					kept = false
					break
				}
			}
			if !kept {
				for remGID, _ := range newGIDs {
					_, ok := counts_remaining[remGID]
					if !ok {
						counts_remaining[remGID] = 0
					}
					val := counts_remaining[remGID]
					if val < shardsToBeAddedPerGroup {
						counts_remaining[remGID]++
						newShards[i] = remGID
						break
					}
				}
			} else {
				newShards[i] = currOld
			}
		}
		newConfigNum := sc.lastConfigNum + 1
		//now assign the new config
		newConfig := Config{Num: newConfigNum, Shards: newShards, Groups: newGIDs}
		sc.configs = append(sc.configs, newConfig)
		sc.lastConfigNum++

	}
}
