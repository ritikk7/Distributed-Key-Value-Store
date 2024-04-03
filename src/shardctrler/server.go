package shardctrler

import (
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
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
	dead            int32 // set by Kill()
	lastClientCalls map[int64]int64
	clientReqs      map[int]chan Op
	lastConfigNum   int
	configs         []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type      string
	ClientId  int64
	RequestId int64
	Servers   map[int][]string
	GIDs      []int
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) ReadApplyMessages() {
	for !sc.killed() {
		for msg := range sc.applyCh {
			sc.mu.Lock()

			if !msg.CommandValid {
				sc.mu.Unlock()
				continue
			}

			command := msg.Command.(Op)
			var opResult Op = command

			lastAppliedId, exists := sc.lastClientCalls[command.ClientId]
			if !exists || command.RequestId > lastAppliedId {
				sc.lastClientCalls[command.ClientId] = command.RequestId

				switch command.Type {
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
					oldShardsPerGroup := NShards / numGroups
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

				if ch, ok := sc.clientReqs[msg.CommandIndex]; ok {
					select {
					case ch <- opResult:
					default:
					}
				} else {
					sc.clientReqs[msg.CommandIndex] = make(chan Op, 1)
					sc.clientReqs[msg.CommandIndex] <- opResult
				}
			}

			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if sc.killed() {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	lastAppliedId, ok := sc.lastClientCalls[args.ClientId]
	if ok && args.RequestId <= lastAppliedId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{Type: JOIN, ClientId: args.ClientId, RequestId: args.RequestId, Servers: args.Servers}
	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch, ok := sc.clientReqs[index]
	if !ok {
		ch = make(chan Op, 1)
		sc.clientReqs[index] = ch
	}
	sc.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp.ClientId == args.ClientId && appliedOp.RequestId == args.RequestId {
			reply.Err = OK

		} else {
			reply.WrongLeader = true
		}
	case <-time.After(800 * time.Millisecond):
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if sc.killed() {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	lastAppliedId, ok := sc.lastClientCalls[args.ClientId]
	if ok && args.RequestId <= lastAppliedId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{Type: LEAVE, ClientId: args.ClientId, RequestId: args.RequestId, GIDs: args.GIDs}
	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch, ok := sc.clientReqs[index]
	if !ok {
		ch = make(chan Op, 1)
		sc.clientReqs[index] = ch
	}
	sc.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp.ClientId == args.ClientId && appliedOp.RequestId == args.RequestId {
			reply.Err = OK

		} else {
			reply.WrongLeader = true
		}
	case <-time.After(800 * time.Millisecond):
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
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

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs[0].Num = 0
	sc.lastClientCalls = make(map[int64]int64)
	sc.clientReqs = make(map[int]chan Op)

	return sc
}
