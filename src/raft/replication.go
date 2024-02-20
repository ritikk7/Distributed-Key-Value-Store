package raft

import "fmt"

// syncLogEntries is run by the leader and sends AppendEntries to all followers
func (rf *Raft) syncLogEntries(pId int, term int) {
	// skip myself
	if pId == rf.me {
		return
	}

	// while I'm the leader, ask this peer to append the entry
	//  1- if pId is down; try again
	//  2- if pId's log is not up-to-date; try another index
	//  3- if pId's log is up-to-date; commit the log entry for current term if we got the majority of followers replicated the log entry
	for !rf.killed() {
		rf.mu.Lock()
		if rf.raftState != Leader || rf.currTerm > term {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("STOP sending entries; not a leader or obselete term, state=%v, termSent=%d, currTerm=%d", rf.raftState, term, rf.currTerm))
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// if there is nothing to send; just wait for the new entries to be added to the log
		rf.cond.L.Lock()
		for rf.matchIndex[pId] >= len(rf.logs)-1 {
			rf.cond.Wait()
			continue
		}
		rf.cond.L.Unlock()

		rf.mu.Lock()
		nextIdx := rf.nextIndex[pId]
		if nextIdx >= len(rf.logs) {
			rf.mu.Unlock()
			continue
		}

		prevLogIndex := nextIdx - 1               // index of the prev log entry on my log
		prevLogTerm := rf.logs[prevLogIndex].Term // the term of the prev log entry
		leaderCommit := rf.commitIndex            // the index of latest commited log entry
		leaderId := rf.me

		entries := append([]LogEntry{}, rf.logs[nextIdx])

		rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("Sending the entry to S%d; prevLogIndex=%d, prevLogTerm=%d, entries=%+v, leaderCommit=%d", pId, prevLogIndex, prevLogTerm, entries, leaderCommit))
		rf.mu.Unlock()

		args := AppendEntriesArg{
			Term:         term,
			LeaderId:     leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}
		reply := AppendEntriesReply{}

		ok := rf.peers[pId].Call(APPEND_ENTRIES_RPC, &args, &reply)
		if !ok {
			rf.mu.Lock()
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("S%d didn't respond to my call, termSent=%d, rf.currTerm=%d, state=%v; try again ...", pId, term, rf.currTerm, rf.raftState))
			rf.mu.Unlock()
			continue
		}

		rf.mu.Lock()
		if rf.raftState != Leader {
			rf.mu.Unlock()
			return
		}

		// 1- if the call was made during the previous terms; ignore the response and return
		if rf.currTerm > term {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("Got an old response from S%d from term=%d, currTerm=%d", pId, term, rf.currTerm))
			rf.mu.Unlock()
			return
		}

		// 2- if my term is behind the reply; turn to a follower and update my term
		if reply.Term > rf.currTerm {
			rf.raftState = Follower
			rf.currTerm = reply.Term
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("S%d has a higher term than mine; changed to a follower and updated my term. currTerm=%d, state=%d", pId, rf.currTerm, rf.raftState))
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// the follower appended the entries for nextIndex
			rf.matchIndex[pId] = nextIdx
			rf.nextIndex[pId] += 1
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("S%d appended the log entry; nextIndex[%d]=%d, matchIndex[%d]=%d, myLastLogIndex=%d", pId, pId, rf.nextIndex[pId], pId, rf.matchIndex[pId], len(rf.logs)-1))
		} else {
			// the follower rejected the entry; try another one
			rf.nextIndex[pId] -= 1
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("S%d rejected the log entry; nextIndex[%d]=%d, myLastLogIndex=%d", pId, pId, rf.nextIndex[pId], len(rf.logs)-1))
		}
		rf.mu.Unlock()

		if reply.Success {
			rf.commitEntries(term)
		}
	}
}

// commitEntries commits all uncommited entry in the leader's log
func (rf *Raft) commitEntries(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term < rf.currTerm || rf.raftState != Leader {
		rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("SKIP Counting because I'm not the leader or this goroutine is obsolete, term=%d, rf.currTerm=%d, state=%d", term, rf.currTerm, rf.raftState))
	}

	maxMatchIdx := -1
	majorityCount := make(map[int]int, 0)
	for _, mIdx := range rf.matchIndex {
		if mIdx == 0 || rf.logs[mIdx].Term != rf.currTerm {
			continue
		}
		majorityCount[mIdx] += 1
		maxMatchIdx = Max(maxMatchIdx, mIdx)
	}

	majorityThrsh := len(rf.peers) / 2

	highestIdx := -1
	for N := maxMatchIdx; N >= 0; N -= 1 {
		if cnt, ok := majorityCount[N]; ok && cnt >= majorityThrsh {
			highestIdx = N
			break
		}
	}

	rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("COUNTING_DONE! Should I apply the command? leaderId=%d, highestIdx=%d, lastApplied=%d, majorityCount=%+v, threshold=%d", rf.leaderId, highestIdx, rf.lastApplied, majorityCount, majorityThrsh))

	if highestIdx != -1 && rf.lastApplied+1 <= highestIdx && highestIdx > rf.commitIndex && highestIdx < len(rf.logs) {
		if len(rf.logs) < 10 {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("\t\tAPPLYING the command to the SM from %d to %d ..., leaderId=%d, rf.commitIndex=%d, len(logs)=%d, logs=%v", rf.lastApplied, highestIdx, rf.leaderId, rf.commitIndex, len(rf.logs), rf.logs))
		} else {
			last5Idx := len(rf.logs) - 6
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("\t\tAPPLYING the command to the SM from %d to %d ..., leaderId=%d, rf.commitIndex=%d, len(logs)=%d, last5_logs=%v", rf.lastApplied, highestIdx, rf.leaderId, rf.commitIndex, len(rf.logs), rf.logs[:last5Idx]))
		}
		for i := rf.lastApplied + 1; i <= highestIdx; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       rf.logs[i].Command,
				CommandIndex:  i,
				SnapshotValid: false,
				Snapshot:      []byte{},
				SnapshotTerm:  term,
				SnapshotIndex: 0,
			}
			rf.lastApplied += 1
			rf.commitIndex += 1
		}

		rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("APPLIED all indices up to %d; lastApplied=%d, commitIndex=%d", highestIdx, rf.lastApplied, rf.commitIndex))
	}
}
