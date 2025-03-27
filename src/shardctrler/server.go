// Refactored shardctrler/server.go for readability and maintainability
package shardctrler

import (
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ==============================
// Structs and Types
// ==============================

type ShardCtrler struct {
	mu              sync.Mutex
	me              int
	rf              *raft.Raft
	applyCh         chan raft.ApplyMsg
	logger          *Logger
	dead            int32
	lastSeenRequest map[int64]int64
	pendingOps      map[int64]chan Op
	configs         []Config
	lastApplied     int
}

type Op struct {
	Type      string
	ClientId  int64
	RequestId int64
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	Num       int
}

// ==============================
// Lifecycle
// ==============================

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	logger, err := NewLogger(1)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}

	sc := &ShardCtrler{
		me:              me,
		logger:          logger,
		applyCh:         make(chan raft.ApplyMsg),
		lastSeenRequest: make(map[int64]int64),
		pendingOps:      make(map[int64]chan Op),
		configs:         make([]Config, 1),
	}

	sc.configs[0] = Config{Num: 0, Groups: map[int][]string{}}
	for i := range sc.configs[0].Shards {
		sc.configs[0].Shards[i] = 0
	}

	labgob.Register(Op{})
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	go sc.runApplyLoop()
	return sc
}

// ==============================
// RPC Helpers
// ==============================

func (sc *ShardCtrler) startOpAndWait(op Op, clientId int64) (Op, bool) {
	sc.mu.Lock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		sc.mu.Unlock()
		return Op{}, false
	}

	ch := make(chan Op, 1)
	sc.pendingOps[clientId] = ch
	sc.rf.Start(op)
	sc.mu.Unlock()

	timer := time.NewTimer(WaitTimeOut)
	defer timer.Stop()

	select {
	case result := <-ch:
		if result.ClientId == op.ClientId && result.RequestId == op.RequestId {
			return result, true
		}
		return Op{}, false
	case <-timer.C:
		return Op{}, false
	}
}

// ==============================
// RPC Handlers
// ==============================

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{Type: JOIN, ClientId: args.ClientId, RequestId: args.RequestId, Servers: args.Servers}
	_, ok := sc.startOpAndWait(op, args.ClientId)
	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrTimeOut
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{Type: LEAVE, ClientId: args.ClientId, RequestId: args.RequestId, GIDs: args.GIDs}
	_, ok := sc.startOpAndWait(op, args.ClientId)
	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrTimeOut
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{Type: MOVE, ClientId: args.ClientId, RequestId: args.RequestId, Shard: args.Shard, GID: args.GID}
	_, ok := sc.startOpAndWait(op, args.ClientId)
	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrTimeOut
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{Type: QUERY, ClientId: args.ClientId, RequestId: args.RequestId, Num: args.Num}
	_, ok := sc.startOpAndWait(op, args.ClientId)
	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrTimeOut
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	if args.Num == -1 || args.Num >= len(sc.configs) {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[args.Num]
	}
	reply.Err = OK
}

// ==============================
// Raft Apply Loop
// ==============================

func (sc *ShardCtrler) runApplyLoop() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if msg.CommandValid {
			sc.mu.Lock()
			if msg.CommandIndex <= sc.lastApplied {
				sc.mu.Unlock()
				continue
			}
			op := msg.Command.(Op)
			lastSeen, seen := sc.lastSeenRequest[op.ClientId]
			if !seen || op.RequestId > lastSeen {
				sc.applyOperation(op)
				sc.lastSeenRequest[op.ClientId] = op.RequestId
			}
			if ch, ok := sc.pendingOps[op.ClientId]; ok {
				select {
				case ch <- op:
				default:
				}
			}
			sc.lastApplied = msg.CommandIndex
			sc.mu.Unlock()
		}
	}
}

// ==============================
// Operation Application
// ==============================

func (sc *ShardCtrler) applyOperation(op Op) {
	switch op.Type {
	case JOIN:
		sc.applyJoin(op)
	case LEAVE:
		sc.applyLeave(op)
	case MOVE:
		sc.applyMove(op)
	case QUERY:
		// no-op
	}
}

func (sc *ShardCtrler) applyJoin(op Op) {
	old := sc.configs[len(sc.configs)-1]
	newGroups := deepCopyGroups(old.Groups)
	for gid, servers := range op.Servers {
		newGroups[gid] = servers
	}
	gids := sortedGIDs(newGroups)
	newConfig := Config{
		Num:    old.Num + 1,
		Groups: newGroups,
		Shards: rebalanceShards(gids),
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyLeave(op Op) {
	old := sc.configs[len(sc.configs)-1]
	leaving := make(map[int]bool)
	for _, gid := range op.GIDs {
		leaving[gid] = true
	}
	newGroups := make(map[int][]string)
	for gid, servers := range old.Groups {
		if !leaving[gid] {
			newGroups[gid] = servers
		}
	}
	gids := sortedGIDs(newGroups)
	var newShards [NShards]int
	if len(gids) > 0 {
		newShards = rebalanceShards(gids)
	}
	sc.configs = append(sc.configs, Config{
		Num:    old.Num + 1,
		Groups: newGroups,
		Shards: newShards,
	})
}

func (sc *ShardCtrler) applyMove(op Op) {
	old := sc.configs[len(sc.configs)-1]
	newShards := old.Shards
	newShards[op.Shard] = op.GID
	sc.configs = append(sc.configs, Config{
		Num:    old.Num + 1,
		Groups: deepCopyGroups(old.Groups),
		Shards: newShards,
	})
}

// ==============================
// Helpers
// ==============================

func rebalanceShards(gids []int) [NShards]int {
	var shards [NShards]int
	sort.Ints(gids)
	perGroup := NShards / len(gids)
	extra := NShards % len(gids)
	idx := 0
	for i, gid := range gids {
		num := perGroup
		if i < extra {
			num++
		}
		for j := 0; j < num; j++ {
			shards[idx] = gid
			idx++
		}
	}
	return shards
}

func deepCopyGroups(groups map[int][]string) map[int][]string {
	copy := make(map[int][]string)
	for gid, servers := range groups {
		copy[gid] = append([]string{}, servers...)
	}
	return copy
}

func sortedGIDs(groups map[int][]string) []int {
	var gids []int
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	return gids
}
