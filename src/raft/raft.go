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
	APPEND_ENTRIES_RPC = "Raft.AppendEntries"
	REQUEST_VOTE_RPC   = "Raft.RequestVote"
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
	logs        []LogEntry // keeps the logs of the current Raft
	commitIndex int        // index of highest log entry known to be commited (initalized to be 0)
	lastApplied int        // index of highes log entry known to be applied to the SM (initalized to be 0)
	leaderId    int        // index of the leader for Followers to redirect the requests to

	// specific to the leader; must be re-initialized after election
	nextIndex  []int // for each server, the index of the next log entry to send to that server (initialiazed to last log index + 1)
	matchIndex []int // for each server, the index of the next log entry to be _replicated_ on other servers (initialized to be 0)

	// channel to pass results to the server
	applyCh chan ApplyMsg
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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// rf.mu.Lock()
	e.Encode(rf.currTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	// rf.mu.Unlock()

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currTerm, votedFor int
	var logs []LogEntry

	if d.Decode(&currTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		fmt.Println("COULDN'T DECODE THE STATE")
	}
	// rf.mu.Lock()
	rf.currTerm = currTerm
	rf.votedFor = votedFor
	rf.logs = append([]LogEntry{}, logs...)
	// rf.mu.Unlock()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
		rf.logger.Log(LogTopicElection, fmt.Sprintf("VOTE REJECTED: S%d asked for a vote but has a lower term! (args.Term=%d rf.currTerm=%d)", args.CandId, args.Term, rf.currTerm))
		reply.VoteGranted = false
		reply.Term = rf.currTerm
		return
	}

	// if the candidate's term is higher than mine:
	//		1- update my term to the candidate's term
	// 		2- turn into a follower
	//		3- reset my vote
	if args.Term > rf.currTerm {
		rf.logger.Log(LogTopicElection, fmt.Sprintf("VOTING: turnning to a follower because S%d has a higher than me! (args.Term=%d, rf.currTerm=%d)", args.CandId, args.Term, rf.currTerm))
		rf.raftState = Follower
		rf.currTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// the candidate's get the vote if:
	//	1- I haven't voted before
	//	2- its log is more up-to-date than mine
	if rf.votedFor >= 0 && rf.votedFor != args.CandId {
		reply.Term = rf.currTerm
		reply.VoteGranted = false
		return
	}

	// the candidate has a more up-to-date log than me if:
	//	1- my last log entry has a lower term than the candidate's last log term; or
	//	2- if the last log terms matches betweem mine and the candidate, if the candidate has a longer log
	lastLogIdx := len(rf.logs) - 1
	if rf.logs[lastLogIdx].Term < args.LastLogTerm {
		rf.logger.Log(LogTopicElection, fmt.Sprintf("VOTE GRANTED! S%d's last entry has a higher term than me! (args.Term=%d, rf.currTerm=%d) (rf.logs[lastLogIdx].Term=%d, args.LastLogTerm=%d)\n\tlogs=%v", args.CandId, args.Term, rf.currTerm, rf.logs[lastLogIdx].Term, args.LastLogTerm, rf.logs))

		rf.votedFor = args.CandId
		rf.persist()

		reply.VoteGranted = true
		reply.Term = rf.currTerm

		return
	} else if rf.logs[lastLogIdx].Term == args.LastLogTerm && args.LastLogIdx >= lastLogIdx {
		rf.logger.Log(LogTopicElection, fmt.Sprintf("VOTE GRANTED! S%d's log is >= than me; (args.Term=%d, rf.currTerm=%d) (args.LastLogIdx=%d, lastLogIdx=%d)\n\tlogs=%v", args.CandId, args.Term, rf.currTerm, args.LastLogIdx, lastLogIdx, rf.logs))

		rf.votedFor = args.CandId
		rf.persist()

		reply.VoteGranted = true
		reply.Term = rf.currTerm

		return
	}

	rf.logger.Log(LogTopicElection, fmt.Sprintf("VOTE REJECTED! my log is more up-to-date than S%d! (args.Term=%d, rf.currTerm=%d) (rf.votedFor=%d) (args.LastLogIndex=%d, args.LastLogTerm=%d)\n\tlog=%v", args.CandId, args.Term, rf.currTerm, rf.votedFor, args.LastLogIdx, args.LastLogTerm, rf.logs))

	reply.Term = rf.currTerm
	reply.VoteGranted = false
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

	index := len(rf.logs)              // the command will be appeared at this index on the leader's log
	term := int(rf.currTerm)           // the leader current term for this command
	isLeader := rf.raftState == Leader // if the server believes it's a leader

	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}

	// append the command to my log
	rf.cond.L.Lock()
	rf.logs = append(rf.logs, LogEntry{
		Term:    term,
		Command: command,
	})
	rf.persist()

	rf.logger.Log(LogTopicStartCmd, fmt.Sprintf("Added the new command to my log!\n\tcommand=%v\n\tlog=%v", command, rf.logs))
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
	XIsShort bool // if the follower's log is shorter
	XLen     int  // the length of the follower's log
	XTerm    int  // the conflicting term in the follower's log
	XIndex   int  // the index of the first entry of XTerm
	Term     int  // currTerm, for leader to update itself
	Success  bool // true if the follower contined the prevLogIndex and preLogTerm
}

// fillReplyX finds the term and its first index in the log for the follower!
// Optimizations required for Lab2C
// Follower:
//
//	XTerm:  term in the conflicting entry (if any)
//	XIndex: index of first entry with that term (if any)
//	XLen:   log length
func (rf *Raft) fillReplyX(reply *AppendEntriesReply, mismatchIdx int, isShorter bool) {
	reply.XIsShort = isShorter
	reply.XLen = len(rf.logs)

	reply.XIndex = mismatchIdx
	reply.XTerm = rf.logs[reply.XIndex].Term

	// find the index where we don't have XTerm
	for reply.XIndex >= 0 && rf.logs[reply.XIndex].Term == reply.XTerm {
		reply.XIndex -= 1
	}
	reply.XIndex += 1
}

// AppendEntries called by the leader either to send a HB or a log entry
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the leader's term lower than mine, I reject the the HB
	if args.Term < rf.currTerm {
		rf.logger.Log(LogTopicAppendEntryRpc, fmt.Sprintf("APE REJECTED: S%d claims to be the leader but has a lower term! (args.Term=%d rf.currTerm=%d)", args.LeaderId, args.Term, rf.currTerm))
		reply.Term = rf.currTerm
		reply.Success = false
		return
	}

	// the leader has a higher term, I update my term and turn into a follower
	if args.Term > rf.currTerm {
		rf.logger.Log(LogTopicAppendEntryRpc, fmt.Sprintf("S%d is the leader with higher term! Turning to a follower. (args.Term=%d rf.currTerm=%d) (leader=S%d)", args.LeaderId, args.Term, rf.currTerm, args.LeaderId))
		rf.raftState = Follower
		rf.currTerm = args.Term
		rf.persist()
	}

	// reset my HB variable and set the leader id for this term
	rf.heartbeat = true
	rf.leaderId = args.LeaderId

	// I reject the call either:
	//   1- my log doesn't have the prevLogIndex or
	//   2- if it does have the prevLogIndex, the term in my log is different
	if !(args.PrevLogIndex < len(rf.logs)) {
		reply.Term = rf.currTerm
		reply.Success = false

		reply.XIsShort = true
		reply.XLen = len(rf.logs)

		rf.logger.Log(LogTopicRejectAppendEntry, fmt.Sprintf("APE REJECTED: my log is shorter than S%d's log! (currTerm=%d, entries=%v) (args.PrevLogIndex=%d, args.PrevLogTerm=%d) (reply.XLen=%d, reply.XIndex=%d, reply.XTerm=%d) (lastLogIndex=%d)\n\tlog=%v", args.LeaderId, rf.currTerm, args.Entries, args.PrevLogIndex, args.PrevLogTerm, reply.XLen, reply.XIndex, reply.Term, len(rf.logs)-1, rf.logs))
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currTerm
		reply.Success = false

		rf.fillReplyX(reply, args.PrevLogIndex, false)

		rf.logger.Log(LogTopicRejectAppendEntry, fmt.Sprintf("APE REJECTED: my prev entry has a mismatched term than S%d! (currTerm=%d, entries=%v) (args.PrevLogIndex=%d, args.PrevLogTerm=%d)  (lastLogIndex=%d) (reply=%+v)\n\tlog=%v", args.LeaderId, rf.currTerm, args.Entries, args.PrevLogIndex, args.PrevLogTerm, len(rf.logs)-1, reply, rf.logs))
		return
	}

	rf.logger.Log(LogTopicMatchPrevApe, fmt.Sprintf("MATCHED Prev Log Entry from S%d; args.PrevLogIndex=%d, args.PrevLogTerm=%d,\n\targs.Entries=%v\n\tlogs=%v", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, rf.logs))

	// finding the conflicting index
	// if it is a HB; conflictIdx is -1
	conflictIdx := 0
	for conflictIdx < len(args.Entries) {
		nextIdx := args.PrevLogIndex + conflictIdx + 1 // next entry in my log
		if nextIdx >= len(rf.logs) {
			// my log is ended
			rf.logger.Log(LogTopicTruncateLogApe, fmt.Sprintf("(LOG Conflict, Leader S%d) There is no entry in my log with index=%d! setting conflictIdx=%d\n\tlog=%v", args.LeaderId, nextIdx, conflictIdx, rf.logs))
			break
		} else if rf.logs[nextIdx].Term != args.Entries[conflictIdx].Term {
			// truncate the log
			rf.logger.Log(LogTopicTruncateLogApe, fmt.Sprintf("(LOG Conflict, Leader S%d) Index %d has a different term than args.Entries[%d]!\n\tShared up to index %d with args.Entries Truncating my log ... (rf.logs[%d].Term=%d, args.Entries[%d].Term=%d)\n\tnew_log=%v\n\tlog=%v\n\targ.Entries=%v", args.LeaderId, nextIdx, conflictIdx, nextIdx, nextIdx, rf.logs[nextIdx].Term, conflictIdx, args.Entries[conflictIdx].Term, rf.logs[:nextIdx], rf.logs, args.Entries))

			rf.logs = rf.logs[:nextIdx]
			rf.persist()
			break
		}
		conflictIdx += 1
	}

	// find which part of the entries have to be added
	rf.logger.Log(LogTopicAppendingEntryApe, fmt.Sprintf("Adding the following portion from args.Entries!\n\tnew_portion=%v\n\targs.Entries=%v", args.Entries[conflictIdx:], args.Entries))
	args.Entries = args.Entries[conflictIdx:] // the portion of the entries we should add to the log; might be nothing

	if len(args.Entries) > 0 {
		// append the new entries
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()

		rf.logger.Log(LogTopicAppendingEntryApe, fmt.Sprintf("Appended the entries to my log! (currTerm=%d)\n\targs.Entries=%v\n\tnew_log=%v", rf.currTerm, args.Entries, rf.logs))
	}

	// if the leader is ahead of me; commit all the entries we haven't commited yet
	if args.LeaderCommit > rf.commitIndex {
		rf.logger.Log(LogTopicUpdateCommitIdxApe, fmt.Sprintf("The leaderCommit(%d) is greater than rf.commitIndex(%d)! updating mine to the minimum(leaderCommit, my_last_log_idx=%d), term=%d", args.LeaderCommit, rf.commitIndex, len(rf.logs)-1, rf.currTerm))

		rf.commitIndex = Min(args.LeaderCommit, len(rf.logs)-1)
	}

	reply.Term = rf.currTerm
	reply.Success = true

	if rf.lastApplied+1 <= rf.commitIndex {
		rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("Uncommited Entries from Leader=S%d -> applying indexes [from=%d to=%d] to the SM\n\tlog=%v", rf.leaderId, rf.lastApplied+1, rf.commitIndex, rf.logs))

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			}
		}
		rf.lastApplied = rf.commitIndex
	} else {
		rf.logger.Log(LogTopicLogUpdateApe, fmt.Sprintf("Nothing to commit! leaderCommit=%d, commitIndex=%d, lastApplied=%d\n\tlog=%v", args.LeaderCommit, rf.commitIndex, rf.lastApplied, rf.logs))
	}

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
	rf.cond.Broadcast()

	rf.logger.Log(LogTopicElection, fmt.Sprintln("I got killed :("))
	time.Sleep(10 * time.Millisecond) // this is ugly; fix it for 2D
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
		logs: []LogEntry{{
			0,
			0,
		}},
		commitIndex: 0,
		lastApplied: 0,
		leaderId:    -1,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		applyCh:     applyCh,
	}
	rf.cond = sync.NewCond(&rf.lock)

	// initialize nextIndex for each server; it has to be 1 at the beginning as it is the next log entry to be sent to them
	for pid := range peers {
		rf.nextIndex[pid] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
