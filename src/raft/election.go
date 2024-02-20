package raft

import (
	"fmt"
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
			prevLogIdx := nextIdx - 1
			prevLogTerm := rf.logs[prevLogIdx].Term

			go rf.sendHB(pId, prevLogIdx, prevLogTerm, leaderCommitIdx, currTerm)
		}
		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}

	rf.logger.Log(LogTopicElection, fmt.Sprintf("stopped sending AppendEntries, currTerm=%d", rf.currTerm))
}

// sendHB calls the AppendEntries RPC of sever pId with empty entries
func (rf *Raft) sendHB(pId, prevLogIdx, prevLogTerm, leaderCommitIdx, currTerm int) {
	if pId == rf.me {
		return
	}

	args := AppendEntriesArg{
		Term:         currTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIdx,
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
			rf.logger.Log(LogTopicHeartbeat, fmt.Sprintf("Got a reply sent during term=%d from S%d which has a higher term %d than currentTerm=%d; turn to a follower ...", currTerm, pId, reply.Term, rf.currTerm))
			rf.raftState = Follower
			rf.currTerm = reply.Term
			return
		}

		if reply.Term > currTerm {
			rf.logger.Log(LogTopicHeartbeat, fmt.Sprintf("HB was rejected by S%d because it was sent during term=%d but the reply has a higher term =%d", pId, currTerm, reply.Term))
			return
		}
	} else {
		rf.logger.Log(LogTopicHeartbeat, fmt.Sprintf("failed to call S%d during term=%d", pId, currTerm))
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

	// 3. ask others to vote for me as well
	args := &RequestVoteArgs{
		Term:        rf.currTerm,
		CandId:      rf.me,
		LastLogIdx:  len(rf.logs) - 1,
		LastLogTerm: rf.logs[len(rf.logs)-1].Term,
	}
	args.Term = rf.currTerm
	args.CandId = rf.me

	rf.logger.Log(LogTopicElection, fmt.Sprintf("starting an election; currTerm=%d", rf.currTerm))

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
					rf.logger.Log(LogTopicElection, fmt.Sprintf("S%d couldn't be reached for a vote term=%d", i, args.Term))
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
		rf.logger.Log(LogTopicElection, fmt.Sprintf("Got the majority. I'm the leader now; currTerm=%d, my_log=%+v", rf.currTerm, rf.logs))

		rf.raftState = Leader
		rf.leaderId = rf.me

		// reset my nextIndex and matchIndex after election
		// nextIndex intialiazed to the last entry's index in the leader's logs
		rf.cond.L.Lock()
		lastLogIndex := len(rf.logs)
		for i := range rf.peers {
			rf.nextIndex[i] = lastLogIndex
			rf.matchIndex[i] = 0
		}
		rf.cond.L.Unlock()

		// start sending HBs
		go rf.startSendingHB()

		// start syncing the entries in the log
		for pId := range rf.peers {
			go rf.syncLogEntries(pId, rf.currTerm)
		}

		rf.mu.Unlock()
	}
}
