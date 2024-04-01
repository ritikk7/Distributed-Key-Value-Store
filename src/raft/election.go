package raft

import (
	"time"
)

// startSendingHB sends HB to each server every TIMEOUT milliseconds
func (rf *Raft) startSendingHB() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.raftState != Leader {
			rf.mu.Unlock()
			return
		}
		currTerm := rf.currTerm
		leaderCommitIdx := rf.commitIndex

		for pId := range rf.peers {
			nextIdx := rf.nextIndex[pId]

			var prevLogIndex int
			var prevLogTerm int

			// normal case
			if rf.GetRelativeIndex(nextIdx) > 0 {
				prevLogIndex = nextIdx - 1                                   // index of the prev log entry on my log
				prevLogTerm = rf.log[rf.GetRelativeIndex(prevLogIndex)].Term // the term of the prev log entry
			} else if rf.GetRelativeIndex(nextIdx) == 0 { // case where prevLogIndex == last included
				prevLogIndex = rf.lastIncludedIndex
				prevLogTerm = rf.lastIncludedTerm
			} else { // case where prevLogIndex < last included
				rf.snapshotCond[pId].Broadcast()
				continue
			}

			Debugf(dTerm, "S%v -> S%v, sending HB, nextIdx %v, snapshotIndex %v, leaderCommit %v\n", rf.me, pId, rf.nextIndex[pId], rf.lastIncludedIndex, leaderCommitIdx)

			go rf.sendHB(pId, prevLogIndex, prevLogTerm, leaderCommitIdx, currTerm)
		}
		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}

	Debugf(dTerm, "S%v stopped sending HB, currTerm %v\n", rf.me, rf.currTerm)
}

// sendHB calls the AppendEntries RPC of sever pId with empty entries
func (rf *Raft) sendHB(pId, prevLogIndex, prevLogTerm, leaderCommitIdx, currTerm int) {
	if pId == rf.me {
		return
	}

	args := AppendEntriesArg{
		Term:         currTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      []LogEntry{},
		LeaderCommit: leaderCommitIdx,
	}
	reply := AppendEntriesReply{}

	ok := rf.peers[pId].Call(APPEND_ENTRIES_RPC, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.raftState != Leader {
			return
		} else if rf.currTerm < reply.Term {
			Debugf(dTerm, "S%v -> S%v, sendHB reply, replyTerm %v > currTerm %v, converting to follower\n", rf.me, pId, reply.Term, rf.currTerm)
			rf.raftState = Follower
			rf.currTerm = reply.Term
			rf.persistState()
			return
		}

		if reply.Term > currTerm {
			Debugf(dTerm, "S%v -> S%v, sendHB reply, replyTerm %v > argsTerm %v\n", rf.me, pId, reply.Term, currTerm)
			return
		}
	} else {
		Debugf(dError, "S%v -> S%v, sendHB failed, no connection, term %v\n", rf.me, pId, rf.currTerm)
	}
}

// startEelction starts an election
func (rf *Raft) startElection() {
	// starting a new election
	rf.mu.Lock()

	// only Followers and Candidates can start elections
	// skip if I'm a leader - might happen when there was a timeout from previous elections or another leader is selected
	if rf.raftState == Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}

	// 0. transition to the Candidate state; reset the timer
	rf.raftState = Candidate
	rf.heartbeat = true // reset my timer

	// 1. increment my term
	rf.currTerm += 1

	// 2. vote for myself
	rf.votedFor = rf.me

	rf.persistState()

	// 3. ask others to vote for me as well
	var lastLogIndex int
	var lastLogTerm int
	if len(rf.log) > 0 {
		lastLogIndex = rf.GetAbsoluteIndex(len(rf.log) - 1)
		lastLogTerm = rf.log[len(rf.log)-1].Term
	} else {
		lastLogIndex = rf.lastIncludedIndex
		lastLogTerm = rf.lastIncludedTerm
	}

	args := &RequestVoteArgs{
		Term:        rf.currTerm,
		CandId:      rf.me,
		LastLogIdx:  lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
	args.Term = rf.currTerm
	args.CandId = rf.me

	Debugf(dLeader, "S%v starting ELECTION, term %v\n", rf.me, rf.currTerm)

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
				ok := rf.peers[i].Call(REQUEST_VOTE_RPC, args, reply)
				if !ok {
					Debugf(dError, "S%v -> S%v, RequestVote failed, no connection, term %v\n", rf.me, i, rf.currTerm)
				}

				// make sure we don't get old responses
				voteCh <- ok && reply.VoteGranted && reply.Term == args.Term
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

	// 1. let's check if there's another server who has already been elected
	rf.mu.Lock()
	if rf.killed() || rf.raftState != Candidate {
		// I'm not a Candidate anymore; we're done with counting
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// Did I get the majority of votes?
	if gotVotes >= majority {

		// change the state to Leader
		rf.mu.Lock()
		Debugf(dLeader, "S%v ELECTION won, term %v\n", rf.me, rf.currTerm)

		rf.raftState = Leader
		rf.leaderId = rf.me

		// reset my nextIndex and matchIndex after election
		// nextIndex intialiazed to the last entry's index in the leader's logs
		rf.cond.L.Lock()
		lastLogIndexRel := len(rf.log)
		for i := range rf.peers {
			rf.nextIndex[i] = rf.GetAbsoluteIndex(lastLogIndexRel)
			rf.matchIndex[i] = 0
		}
		rf.cond.L.Unlock()

		// start sending HBs
		go rf.startSendingHB()

		// start syncing the entries in the log
		for pId := range rf.peers {
			go rf.syncLogEntries(pId, rf.currTerm)
			go rf.sendSnapshots(pId, rf.currTerm)
		}

		rf.mu.Unlock()
	}
}
