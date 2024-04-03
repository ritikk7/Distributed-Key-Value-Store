package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"cpsc416/labgob"
	"cpsc416/labgob"
	"cpsc416/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState int

const (
	Follower = iota
	Candidate
	Leader
)

const (
	APPEND_ENTRIES_RPC   = "Raft.AppendEntries"
	REQUEST_VOTE_RPC     = "Raft.RequestVote"
	INSTALL_SNAPSHOT_RPC = "Raft.InstallSnapshot"
)

// LogEntry represents each log entry in the rf.logs array
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu   sync.Mutex // Lock to protect shared access to this peer's state
	lock sync.Mutex // Lock to protect the shared accessed during log replication
	cond *sync.Cond // Cond variable to synchronized the log replication go routines

	snapshotCond []*sync.Cond

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	logger *Logger

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	raftState   RaftState  // the current state of this Raft (Follower, Candidate, Leader)
	currTerm    int        // current term at this Raft
	votedFor    int        // the peer this Raft voted for during the last election
	heartbeat   bool       // keeps track of the heartbeats
	log         []LogEntry // keeps the logs of the current Raft
	commitIndex int        // index of highest log entry known to be commited (initalized to be 0)
	lastApplied int        // index of highes log entry known to be applied to the SM (initalized to be 0)
	leaderId    int        // index of the leader for Followers to redirect the requests to

	// specific to the leader; must be re-initialized after election
	nextIndex  []int // for each server, the index of the next log entry to send to that server (initialiazed to last log index + 1)
	matchIndex []int // for each server, the index of the next log entry to be _replicated_ on other servers (initialized to be 0)

	// channel to pass results to the server
	applyCh chan ApplyMsg

	// snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
	applying          bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	return int(rf.currTerm), rf.raftState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	wBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(wBuffer)

	// peristent state
	encoder.Encode(rf.currTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)

	// snapshot
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)

	Debugf(dPersist, "S%v encoded persist\n", rf.me)

	raftState := wBuffer.Bytes()
	rf.persister.Save(raftState, snapshot)
}

func (rf *Raft) persistState() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	wBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(wBuffer)

	// peristent state
	encoder.Encode(rf.currTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)

	// snapshot
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)

	Debugf(dPersist, "S%v encoded persist\n", rf.me)

	raftState := wBuffer.Bytes()
	rf.persister.SaveState(raftState)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	rBuffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(rBuffer)

	// peristent state
	var currTerm int
	var votedFor int
	var log []LogEntry

	// snapshot
	var lastIncludedIndex int
	var lastIncludedTerm int

	if decoder.Decode(&currTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil ||
		decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&lastIncludedTerm) != nil {
		Debugf(dPersist, "S%v unable to decode for persist\n", rf.me)
	} else {
		Debugf(dPersist, "S%v decoded persist\n", rf.me)

		rf.currTerm = currTerm
		rf.votedFor = votedFor
		rf.log = log

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	Debugf(dSnap, "S%v grabbing lock for snapshot for [%v], lastIncludedIndex %v, relativeLogLength %v\n", rf.me, index, rf.lastIncludedIndex, len(rf.log))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debugf(dSnap, "S%v grabbed lock for snapshot for [%v], lastIncludedIndex %v, relativeLogLength %v\n", rf.me, index, rf.lastIncludedIndex, len(rf.log))

	if index <= rf.lastIncludedIndex {
		Debugf(dSnap, "S%v aborting snapshot up to [%v], index %v, lastIncludedIndex %v\n", rf.me, index, index, rf.lastIncludedIndex)
		return
	}

	Debugf(dSnap, "S%v attempting snapshot up to [%v], lastIncludedIndex %v, relativeLogLength %v\n", rf.me, index, rf.lastIncludedIndex, len(rf.log))

	truncIndex := rf.GetRelativeIndex(index + 1)
	lastIncludedTerm := rf.log[rf.GetRelativeIndex(index)].Term

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = lastIncludedTerm

	rf.log = rf.log[truncIndex:]
	rf.persistStateAndSnapshot(snapshot)

	Debugf(dSnap, "S%v created snapshot, lastIncludedIndex %v, relativeLogLength %v\n", rf.me, rf.lastIncludedIndex, len(rf.log))

	if rf.raftState == Leader {
		for pId := range rf.peers {
			if rf.GetRelativeIndex(rf.nextIndex[pId]) < 0 {
				rf.snapshotCond[pId].Broadcast()
			}
		}
	}
}

type InstallSnapshotArg struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapShotReply struct {
	Term              int
	LastIncludedIndex int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArg, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the leader's term lower than mine, I reject the install
	if args.Term < rf.currTerm {
		Debugf(dTerm, "S%v <- S%v installSnapshot rejected, argsTerm %v < currTerm %v\n", rf.me, args.LeaderId, args.Term, rf.currTerm)
		reply.Term = rf.currTerm
		return
	}

	// stale snapshot
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		Debugf(dTerm, "S%v <- S%v installSnapshot rejected, argsLastIncludedIndex %v <= lastIncludedIndex %v\n", rf.me, args.LeaderId, args.LastIncludedIndex, rf.lastIncludedIndex)
		reply.LastIncludedIndex = rf.lastIncludedIndex
		return
	}

	if rf.applying {
		Debugf(dTerm, "S%v <- S%v installSnapshot aborting, already currently applying %v\n", rf.me, args.LeaderId)
		return
	}

	lastLogIndex := rf.GetAbsoluteIndex(len(rf.log) - 1)
	if args.LastIncludedIndex <= lastLogIndex {
		rf.log = rf.log[rf.GetRelativeIndex(args.LastIncludedIndex+1):]
	} else {
		rf.log = []LogEntry{}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// stale apply index
	if args.LastIncludedIndex <= rf.lastApplied {
		Debugf(dTerm, "S%v <- S%v installSnapshot not applying, stale, argsLastApplied %v <= lastApplied %v\n", rf.me, args.LeaderId, args.LastIncludedIndex, rf.lastApplied)
		rf.persistStateAndSnapshot(args.Data)
		return
	}

	Debugf(dSnap, "S%v <- S%v installSnapshot applying snapshot, lastApplied [%v] -> [%v]\n", rf.me, args.LeaderId, rf.lastApplied, args.LastIncludedIndex)

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = Max(args.LastIncludedIndex, rf.commitIndex)

	rf.persistStateAndSnapshot(args.Data)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandId      int
	LastLogIdx  int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote called by Candidates to ask for the peer's vote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the candidate's term is smaller than mine, I reject the vote
	if args.Term < rf.currTerm {
		Debugf(dTerm, "S%v <- S%v, requestVote rejected, argsTerm %v < currTerm %v\n", rf.me, args.CandId, args.Term, rf.currTerm)
		reply.VoteGranted = false
		reply.Term = rf.currTerm
		return
	}

	// if the candidate's term is higher than mine:
	//		1- update my term to the candidate's term
	// 		2- turn into a follower
	//		3- reset my vote
	if args.Term > rf.currTerm {
		Debugf(dTerm, "S%v <- S%v, argsTerm %v > currTerm %v, converting to follower\n", rf.me, args.CandId, args.Term, rf.currTerm)
		rf.currTerm = args.Term
		rf.raftState = Follower
		rf.votedFor = -1
		rf.persistState()
	}

	// the candidate's get the vote if:
	//	1- I haven't voted before
	//	2- its log is more up-to-date than mine
	if rf.votedFor >= 0 && rf.votedFor != args.CandId {
		Debugf(dVote, "S%v <- S%v, requestVote reject, voted for %v\n", rf.me, args.CandId, rf.votedFor)
		reply.Term = rf.currTerm
		reply.VoteGranted = false
		return
	}

	// the candidate has a more up-to-date log than me if:
	//	1- my last log entry has a lower term than the candidate's last log term; or
	//	2- if the last log terms matches betweem mine and the candidate, if the candidate has a longer log

	var lastLogIdx int
	var lastLogTerm int

	if len(rf.log) > 0 {
		lastLogIdx = rf.GetAbsoluteIndex(len(rf.log) - 1)
		lastLogTerm = rf.log[rf.GetRelativeIndex(lastLogIdx)].Term
	} else {
		lastLogIdx = rf.lastIncludedIndex
		lastLogTerm = rf.lastIncludedTerm
	}

	if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && args.LastLogIdx >= lastLogIdx) {
		Debugf(dVote, "S%v <- S%v, requestVote granted\n", rf.me, args.CandId)

		rf.votedFor = args.CandId
		rf.persistState()

		reply.VoteGranted = true
		reply.Term = rf.currTerm

		return
	}

	Debugf(dVote, "S%v <- S%v, requestVote rejected, lastLogTerm %v > argsLastLogTerm %v OR argsLastLogIndex [%v] < lastLogIndex [%v]\n", rf.me, args.CandId, args.LastLogTerm, lastLogTerm, args.LastLogIdx, lastLogIdx)

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.GetAbsoluteIndex(len(rf.log)) // the command will be appeared at this index on the leader's log
	term := int(rf.currTerm)                  // the leader current term for this command
	isLeader := rf.raftState == Leader        // if the server believes it's a leader

	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}

	// append the command to my log
	rf.cond.L.Lock()
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})
	rf.persistState()

	Debugf(dClient, "S%v appended command %v [%v] to log, log: %v\n", rf.me, command, index, rf.log)
	rf.cond.L.Unlock()

	// notify go routines to sync the logs
	rf.cond.Broadcast()

	return index, term, isLeader
}

type AppendEntriesArg struct {
	Term         int        // leader's term
	LeaderId     int        // leader's id so that followers can redirect them
	PrevLogIndex int        // index of previous log entry preceeding the new one
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for HB, more than one for logs)
	LeaderCommit int        // leader's commitIndex
}
type AppendEntriesReply struct {
	Term    int  // currTerm, for leader to update itself
	Success bool // true if the follower contined the prevLogIndex and preLogTerm

	// 2C rejection efficiency
	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

// AppendEntries called by the leader either to send a HB or a log entry
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the leader's term lower than mine, I reject the the HB
	if args.Term < rf.currTerm {
		Debugf(dTerm, "S%v <- S%v appendEntries rejected, argsTerm %v < currTerm %v\n", rf.me, args.LeaderId, args.Term, rf.currTerm)
		reply.Term = rf.currTerm
		reply.Success = false

		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = -1

		return
	}

	// the leader has a higher term, I update my term and turn into a follower
	if args.Term > rf.currTerm {
		Debugf(dTerm, "S%v <- S%v, argsTerm %v > currTerm %v, converting to follower\n", rf.me, args.LeaderId, args.Term, rf.currTerm)
		rf.raftState = Follower
		rf.currTerm = args.Term
		rf.persistState()
	}

	// reset my HB variable and set the leader id for this term
	rf.heartbeat = true
	rf.leaderId = args.LeaderId

	// I reject the call either:
	//   1- my log doesn't have the prevLogIndex or
	if !(args.PrevLogIndex < rf.GetAbsoluteIndex(len(rf.log))) {
		reply.Term = rf.currTerm
		reply.Success = false

		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.GetAbsoluteIndex(len(rf.log))

		Debugf(dLog, "S%v <- S%v appendEntries rejected, log not long enough, argsPrevLog term %v [%v], argsEntries: %v, lastLogIndex [%v], log: %v\n", rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, args.Entries, rf.GetAbsoluteIndex(len(rf.log)-1), rf.log)
		return
	}
	//   2- if it does have the prevLogIndex, the term in my log is different
	if rf.GetRelativeIndex(args.PrevLogIndex) > -1 { // normal case
		if rf.log[rf.GetRelativeIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
			reply.Term = rf.currTerm
			reply.Success = false

			XTerm := rf.log[rf.GetRelativeIndex(args.PrevLogIndex)].Term
			// find the first index of PrevLogTerm
			iRel := 0
			for rf.log[iRel].Term != XTerm {
				iRel++
			}
			// todo investigate irel == -1 (lastincludedterm)
			reply.XTerm = XTerm
			reply.XIndex = rf.GetAbsoluteIndex(iRel)
			reply.XLen = rf.GetAbsoluteIndex(len(rf.log))

			Debugf(dLog, "S%v <- S%v appendEntries rejected, term at index inconsitency, argsPrevLog term %v [%v] != log term %v [%v], argsEntries: %v, lastLogIndex [%v], log: %v\n", rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.log[rf.GetRelativeIndex(args.PrevLogIndex)].Term, args.PrevLogIndex, args.Entries, rf.GetAbsoluteIndex(len(rf.log)-1), rf.log)
			return
		}
	} else if rf.GetRelativeIndex(args.PrevLogIndex) == -1 { // case where prevLogIndex == -1
		if rf.lastIncludedTerm != args.PrevLogTerm {
			reply.Term = rf.currTerm
			reply.Success = false

			lastIncludedIndex := rf.lastIncludedIndex
			lastIncludedTerm := rf.lastIncludedTerm

			reply.XTerm = lastIncludedTerm
			reply.XIndex = lastIncludedIndex
			reply.XLen = rf.GetAbsoluteIndex(len(rf.log))

			Debugf(dLog, "S%v <- S%v appendEntries rejected, term at index inconsitency, argsPrevLog term %v [%v] != log term %v [%v], argsEntries: %v, lastLogIndex [%v], log: %v\n", rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.log[rf.GetRelativeIndex(args.PrevLogIndex)].Term, args.PrevLogIndex, args.Entries, rf.GetAbsoluteIndex(len(rf.log)-1), rf.log)
			return
		}
	} else {
		reply.Term = rf.currTerm
		reply.Success = false

		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.GetAbsoluteIndex(len(rf.log))

		Debugf(dLog, "S%v <- S%v appendEntries rejected, argsPrevLogIndex %v < lastIncludedIndex %v\n", args.PrevLogIndex, rf.lastIncludedIndex)
		return
	}

	// check if I have the new entry in my log:
	//	1- if I have it, it has to have the same term as mine
	//	2- if I have it, but it has a different term, I'll truncate my log and update it with the leader's entries
	if len(args.Entries) > 0 {

		entriesIndex := 0
		logIndex := args.PrevLogIndex + 1

		for entriesIndex < len(args.Entries) && logIndex < rf.GetAbsoluteIndex(len(rf.log)) {
			// check for term mismatch
			if args.Entries[entriesIndex].Term != rf.log[rf.GetRelativeIndex(logIndex)].Term {
				// truncate and append starting at mismatch index
				Debugf(dLog, "S%v <- S%v appendEntries log truncated, log[%v].Term %v != leaderLog[%v].Term %v , log: %v\n", rf.me, args.LeaderId, logIndex, rf.log[rf.GetRelativeIndex(logIndex)].Term, logIndex, args.Entries[entriesIndex].Term, rf.log)
				rf.log = append(rf.log[:rf.GetRelativeIndex(logIndex)], args.Entries[entriesIndex:]...)
				rf.persistState()
				goto doneAppend
			}
			entriesIndex++
			logIndex++
		}
		// end of one of lists reached with no term mismatch
		lastLogIndex := rf.GetAbsoluteIndex(len(rf.log) - 1)
		Debugf(dLog, "S%v <- S%v appendEntries log entries match, lastLogIndex [%v], log: %v\n", rf.me, args.LeaderId, lastLogIndex, rf.log)

		// append if entries has extra entries I don't have
		if logIndex == rf.GetAbsoluteIndex(len(rf.log)) {
			Debugf(dLog, "S%v <- S%v appendEntries appending at [%v], entries %v to log, log: %v\n", rf.me, args.LeaderId, logIndex, args.Entries, rf.log)
			rf.log = append(rf.log, args.Entries[entriesIndex:]...)
			rf.persistState()
		}
	}

doneAppend:

	// check if this AppendEntries actually verified up to the end of the log
	// note that Entries will be empty when prevLogIndex is already highest value
	// ie. not a HB (HB does not do log verification, but may raise leaderCommit)

	if rf.applying {
		Debugf(dCommit, "S%v already applying, aborting\n", rf.me)
		return
	}

	rf.applying = true

	if len(args.Entries) > 0 || args.PrevLogIndex == rf.GetAbsoluteIndex(len(rf.log)-1) {
		// if the leader is ahead of me; commit all the entries we haven't commited yet
		if args.LeaderCommit > rf.commitIndex {
			Debugf(dCommit, "S%v <- S%v, updating commitIndex [%v] to min(leaderCommit [%v], lastLogIndex [%v])\n", rf.me, args.LeaderId, rf.commitIndex, args.LeaderCommit, rf.GetAbsoluteIndex(len(rf.log)-1))
			rf.commitIndex = Min(args.LeaderCommit, rf.GetAbsoluteIndex(len(rf.log)-1))
		}

		reply.Term = rf.currTerm
		reply.Success = true

		if rf.lastApplied+1 <= rf.commitIndex {
			Debugf(dCommit, "S%v <- S%v, begin applying commands [%v] to [%v], log: %v\n", rf.me, args.LeaderId, rf.lastApplied+1, rf.commitIndex, rf.log)

			for iAbs := rf.lastApplied + 1; iAbs <= rf.commitIndex; iAbs++ {
				Debugf(dCommit, "S%v applying at [%v], %v\n", rf.me, iAbs, rf.log[rf.GetRelativeIndex(iAbs)].Command)

				command := rf.log[rf.GetRelativeIndex(iAbs)].Command
				rf.mu.Unlock()

				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      command,
					CommandIndex: iAbs,
				}

				Debugf(dCommit, "S%v applied at [%v], grabbing lock again\n", rf.me, iAbs)
				// snapshot applied higher than you, can stop
				rf.mu.Lock()
				Debugf(dCommit, "S%v applied at [%v], grabbed lock again\n", rf.me, iAbs)
			}

			rf.lastApplied = rf.commitIndex

		} else {
			Debugf(dCommit, "S%v <- S%v nothing to commit, lastApplied [%v], commitIndex [%v]\n", rf.me, args.LeaderId, rf.lastApplied, rf.commitIndex)
		}
	}

	rf.applying = false
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	var ms int64
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		// avoids the cold start vote split situation
		ms = 250 + (rand.Int63() % 250)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// check if we got a heartbeat from the leader
		// if we haven't recieved any hearts; start an election
		rf.mu.Lock()
		if !rf.heartbeat && !rf.killed() {
			go rf.startElection()
		}
		// reset the heartbeat
		rf.heartbeat = false
		rf.mu.Unlock()

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	logger, err := NewLogger(me)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)
	}

	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		mu:        sync.Mutex{},
		lock:      sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		logger:    logger,
		raftState: Follower,
		currTerm:  0,
		votedFor:  -1,
		heartbeat: false,
		log: []LogEntry{{
			0,
			0,
		}},
		commitIndex:       0,
		lastApplied:       0,
		leaderId:          -1,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		applyCh:           applyCh,
		lastIncludedIndex: -1,
		lastIncludedTerm:  -1,
		snapshotCond:      make([]*sync.Cond, len(peers)),
	}
	rf.cond = sync.NewCond(&rf.lock)
	for i := range peers {
		rf.snapshotCond[i] = sync.NewCond(&rf.lock)
	}

	// initialize nextIndex for each server; it has to be 1 at the beginning as it is the next log entry to be sent to them
	for pid := range peers {
		rf.nextIndex[pid] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
