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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "cpsc416/labgob"
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

type LogEntry struct {
	term    int
	index   int         // first index = 1
	command interface{} // command for log entry
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	leaderId  int                 // the id of the leader for the current term

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	raftState   RaftState  // the current state of this Raft (Follower, Candidate, Leader)
	currTerm    int        // current term at this Raft
	votedFor    int        // the peer this Raft voted for during the last election
	heartbeat   bool       // keeps track of the heartbeats
	logs        []LogEntry // list of LogEntry
	commitIndex int        // index of highest log entry known to be committed
	lastApplied int        // index of highest log entry applied to state machine
	nextIndex   []int      // index of the next log entry to send to that server
	matchIndex  []int      // index of highest log entry known to be replicated on server
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).

	// I can vote if:
	// 1 - the term of the requester is >= of my term
	// 2 - I haven't voted for the requested term before

	// if the requester term is behind me, it means that the requester is out of sync; I reject the vote
	if args.Term < rf.currTerm {
		reply.VoteGranted = false
		reply.Term = rf.currTerm
		return
	}

	// if the requester last log term/index is less than my last log term/index: reject
	if (args.LastLogTerm < rf.logs[len(rf.logs)-1].term) || (args.LastLogIdx < rf.logs[len(rf.logs)-1].index) {
		reply.VoteGranted = false
		return
	}

	// if the requester term is more than me, it means that it is an election period; I grant the vote
	if args.Term > rf.currTerm {
		rf.currTerm = args.Term // reset my term to the new one
		rf.raftState = Follower // reset my state to Follower until the election ends or I become a Candidate
		rf.votedFor = -1        // reset my vote
	}

	// rf.votedFor < 0: it's a new term; I should grant the vote if I haven't granted my vote to someone else
	if rf.votedFor < 0 || rf.votedFor == args.CandId {
		rf.votedFor = args.CandId
		reply.VoteGranted = true
	}

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
	index := -1
	term := -1
	isLeader := rf.me == rf.leaderId

	// if not leader, return false
	if !isLeader {
		return -1, -1, false
	}

	// 5.3: The leader appends the command to its log as a new entry,
	// then issues AppendEntries RPCs in parallel to each of the
	// other servers to replicate the entry

	// TODO: is this correct?
	// add command to log
	term = rf.currTerm
	index = len(rf.logs) + 1
	logEntry := LogEntry{term: term, index: index, command: command}
	rf.logs[index-1] = logEntry

	// issue AppendEntries RPC to followers
	// 5.3: leader must find the latest log entry where the two
	// logs agree, delete any entries in the follower’s log after
	// that point, and send the follower all of the leader’s entries
	// after that point
	for i, peer := range rf.peers {
		if i != rf.me {
			for {
				args := AppendEntriesArg{
					Term:     term,
					LeaderId: rf.me,
					Entries:  rf.logs[rf.nextIndex[i]:],
				}
				reply := AppendEntriesReply{}
				peer.Call("Raft.AppendEntries", &args, &reply)
				if reply.Success == true {
					break
				}
				rf.nextIndex[i] -= 1
			}
		}
	}

	return index, term, isLeader
}

type AppendEntriesArg struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// HeartBeat reset the timer if it is called by the leader
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currTerm {
		reply.Term = rf.currTerm
		reply.Success = false
		return
	}

	rf.heartbeat = true
	rf.raftState = Follower
	rf.leaderId = args.LeaderId
	rf.currTerm = args.Term

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

func (rf *Raft) startSendingHB() {

	// check if I'm still the leader before sending HBs
	for !rf.killed() && rf.raftState == Leader {
		rf.mu.Lock()
		currTerm := rf.currTerm
		leaderId := rf.me
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				go func(i int) {
					args := &AppendEntriesArg{
						Term:     currTerm,
						LeaderId: leaderId,
					}
					reply := &AppendEntriesReply{}
					ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)

					rf.mu.Lock()
					if ok && reply.Term > rf.currTerm {
						rf.raftState = Follower
						rf.currTerm = reply.Term
					}
					rf.mu.Unlock()
				}(i)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// startEelction starts an election
func (rf *Raft) startElection() {
	// starting a new election
	rf.mu.Lock()

	// only Followers and Candidates can start elections
	// skip if I'm a leader - might happen when there was timeout from previous elections or another leader is selected
	if rf.raftState == Leader {
		rf.mu.Unlock()
		return
	}

	// 0. transition to the Candidate state
	rf.raftState = Candidate

	// 1. increment my term
	rf.currTerm += 1

	// 2. vote for myself
	rf.votedFor = rf.me

	// 3. ask others to vote for me as well
	args := &RequestVoteArgs{}
	args.Term = rf.currTerm
	args.CandId = rf.me

	rf.mu.Unlock()

	// should ask the peers in parallel for their vote;
	// so we'll wait on this channel after sending the requests in parallel
	voteCh := make(chan bool)

	gotVotes := 1                   // gotVotes counts granted votes for me in this round of election; counted my vote already
	majority := len(rf.peers)/2 + 1 // majority is the threshold for winning the current election
	recVotes := 1                   // recVotes counts all peers voted (mine counted); in case we haven't reached a majority of votes

	// asking peers to vote until
	// 1. I win!
	// 2. someone else wins!
	// 3. another timeout happens
	for i := 0; i < len(rf.peers); i += 1 {
		// skip asking myself - already voted
		if i != rf.me {
			go func(i int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				voteCh <- ok && reply.VoteGranted
			}(i)
		}
	}

	// let's count the votes
	for gotVotes < majority && recVotes < len(rf.peers) {
		if <-voteCh {
			gotVotes += 1
		}
		recVotes += 1
	}

	// counting ended; let's see the results
	// 1. let's check if there's another server who has already been elected
	rf.mu.Lock()
	if rf.raftState != Candidate {
		// I'm not a Candidate anymore; we're done with counting
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// Did I get the majority of votes?
	if gotVotes >= majority {
		// change state to Leader
		rf.mu.Lock()
		rf.raftState = Leader
		rf.leaderId = rf.me
		for i, _ := range rf.peers {
			if i != rf.me {
				rf.nextIndex[i] = len(rf.logs) + 1
				rf.matchIndex[i] = 0
			}
		}
		rf.mu.Unlock()

		// start sending HBs
		go rf.startSendingHB()
	}
}

func (rf *Raft) ticker() {
	var ms int64

	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		// avoid the first vote split in the first round of election
		ms = 350 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// check if we got a heartbeat from the leader
		// if we haven't recieved any hearts; start an election
		if !rf.heartbeat {
			go rf.startElection()
		}
		// reset the heartbeat
		rf.heartbeat = false

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

	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		leaderId:  -1,
		raftState: Follower,
		currTerm:  0,
		votedFor:  -1,
		heartbeat: false,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
