package raft

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
			Debugf(dTerm, "S%v stopped syncLogEntries, state %v != leader OR currTerm %v > initialTerm %v\n", rf.me, rf.raftState, rf.currTerm, term)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// if there is nothing to send; just wait for the new entries to be added to the log
		rf.cond.L.Lock()
		for rf.matchIndex[pId] >= rf.GetAbsoluteIndex(len(rf.log)-1) {
			// Debugf(dInfo, "S%v -> S%v spinning, matchIndex [%v] >= lastLogIndex [%v]\n", rf.me, pId, rf.matchIndex[pId], rf.GetAbsoluteIndex(len(rf.log)-1))
			rf.cond.Wait()
			continue
		}
		rf.cond.L.Unlock()

		rf.mu.Lock()
		nextIdx := rf.nextIndex[pId]
		if nextIdx >= rf.GetAbsoluteIndex(len(rf.log)) {
			// Debugf(dInfo, "S%v -> S%v spinning, nextIndex [%v] >= lastLogIndex [%v]\n", rf.me, pId, nextIdx, rf.GetAbsoluteIndex(len(rf.log)-1))
			rf.mu.Unlock()
			continue
		}

		var prevLogIndex int
		var prevLogTerm int
		var entries []LogEntry
		// normal case
		if rf.GetRelativeIndex(nextIdx) > 0 {
			prevLogIndex = nextIdx - 1                                   // index of the prev log entry on my log
			prevLogTerm = rf.log[rf.GetRelativeIndex(prevLogIndex)].Term // the term of the prev log entry
			entries = rf.log[rf.GetRelativeIndex(nextIdx):]
		} else if rf.GetRelativeIndex(nextIdx) == 0 { // case where prevLogIdx == last included
			prevLogIndex = rf.lastIncludedIndex
			prevLogTerm = rf.lastIncludedTerm
			entries = rf.log
		} else { // case where prevLogIdx < last included
			rf.snapshotCond[pId].Broadcast()
			rf.mu.Unlock()
			continue
		}

		leaderCommit := rf.commitIndex // the index of latest commited log entry
		leaderId := rf.me

		Debugf(dLog2, "S%v -> S%v syncLogEntries, prevLogIndex %d, prevLogTerm %v, entries %v, leaderCommit %v\n", rf.me, pId, prevLogIndex, prevLogTerm, entries, leaderCommit)
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
			Debugf(dError, "S%v -> S%v, syncLogEntries failed, no connection, term %v\n", rf.me, pId, rf.currTerm)
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
			Debugf(dTerm, "S%v -> S%v, syncLogEntries reply, currTerm %v> argsTerm %v, current call outdated, stopping\n", rf.me, pId, rf.currTerm, term)
			rf.mu.Unlock()
			return
		}

		// 2- if my term is behind the reply; turn to a follower and update my term
		if reply.Term > rf.currTerm {
			rf.raftState = Follower
			rf.currTerm = reply.Term
			rf.persistState()
			Debugf(dTerm, "S%v -> S%v, syncLogEntries reply, replyTerm %v> currTerm %v, stopping\n", rf.me, pId, reply.Term, rf.currTerm)
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// the follower appended the entries for nextIndex
			rf.matchIndex[pId] = nextIdx + len(entries) - 1
			rf.nextIndex[pId] = nextIdx + len(entries)
			Debugf(dLog2, "S%v -> S%v, syncLogEntries succeeded, nextIndex [%v], matchIndex [%v], leaderCommit %v, lastLogIndex [%v]\n", rf.me, pId, rf.nextIndex[pId], rf.matchIndex[pId], leaderCommit, prevLogIndex+len(entries))
		} else {
			// the follower rejected the entry; try another one

			if reply.XTerm != -1 && reply.XIndex != -1 { // rejected because term mismatch at index
				// find index of last XTerm on our log
				iRel := len(rf.log) - 1
				for iRel >= 0 {
					if rf.log[iRel].Term == reply.XTerm {
						break
					}
					iRel--
				}

				if iRel != -1 { // leader has XTerm
					Debugf(dLog2, "S%v -> S%v, A, nextIndex [%v] -> [%v]\n", rf.me, pId, rf.nextIndex[pId], iRel)
					rf.nextIndex[pId] = rf.GetAbsoluteIndex(iRel)
				} else { // leader doesn't have XTerm
					Debugf(dLog2, "S%v -> S%v, B, nextIndex [%v] -> [%v]\n", rf.me, pId, rf.nextIndex[pId], reply.XIndex)
					rf.nextIndex[pId] = reply.XIndex
				}

			} else if reply.XLen != -1 { // rejected because follower log shorter
				Debugf(dLog2, "S%v -> S%v, C, nextIndex [%v] -> [%v]\n", rf.me, pId, rf.nextIndex[pId], reply.XLen)
				rf.nextIndex[pId] = reply.XLen
			} else { // else rejected for term mismatch, could be stale
				rf.nextIndex[pId] = rf.nextIndex[pId] - 1
			}

			Debugf(dLog2, "S%v -> S%v, syncLogEntries rejected, attempting nextIndex [%v], lastLogIndex [%v]\n", rf.me, pId, rf.nextIndex[pId], rf.GetAbsoluteIndex(len(rf.log)-1))
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

	if rf.applying {
		Debugf(dCommit, "S%v already applying, aborting\n", rf.me)
		return
	}

	if term < rf.currTerm || rf.raftState != Leader {
		Debugf(dLog2, "S%v commitEntries not counting, currTerm %v > initialTerm %v OR state %v != leader\n", rf.me, term, rf.currTerm, rf.raftState)
	}

	maxMatchIdx := -1
	majorityCount := make(map[int]int, 0)
	for _, mIdx := range rf.matchIndex {

		if rf.GetRelativeIndex(mIdx) < 0 {
			continue
		}

		if mIdx == 0 || rf.log[rf.GetRelativeIndex(mIdx)].Term != rf.currTerm {
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

	Debugf(dCommit, "S%v counting complete, highest agreed [%v] by majority %+v of %v\n", rf.me, highestIdx, majorityCount, len(rf.peers))

	rf.applying = true

	if highestIdx != -1 && rf.lastApplied+1 <= highestIdx && highestIdx > rf.commitIndex && highestIdx < rf.GetAbsoluteIndex(len(rf.log)) {
		if len(rf.log) < 10 {
			Debugf(dCommit, "S%v leader begin applying commands [%v] to [%v], log: %v\n", rf.me, rf.lastApplied+1, highestIdx, rf.log)
		} else {
			last5Idx := len(rf.log) - 6
			Debugf(dCommit, "S%v leader begin applying commands [%v] to [%v], log[-5:]: %v\n", rf.me, rf.lastApplied+1, highestIdx, rf.log[last5Idx:])
		}
		startIndex := rf.lastApplied + 1
		for iAbs := startIndex; iAbs <= highestIdx; iAbs++ {
			Debugf(dCommit, "S%v applying at [%v], {%v %v} \n", rf.me, iAbs, term, rf.log[rf.GetRelativeIndex(iAbs)].Command)
			command := rf.log[rf.GetRelativeIndex(iAbs)].Command
			Debugf(dCommit, "S%v lastApplied: %v, commitIndex: %v before applying \n", rf.me, rf.lastApplied, rf.commitIndex)
			rf.mu.Unlock()

			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       command,
				CommandIndex:  iAbs,
				SnapshotValid: false,
				Snapshot:      []byte{},
				SnapshotTerm:  term,
				SnapshotIndex: 0,
			}

			rf.mu.Lock()
			rf.lastApplied = iAbs
			rf.commitIndex = iAbs
			Debugf(dCommit, "S%v lastApplied: %v, commitIndex: %v after applying \n", rf.me, rf.lastApplied, rf.commitIndex)
		}

		Debugf(dCommit, "S%v applied indices up to [%v], lastApplied %v, commitIndex %v\n", rf.me, highestIdx, rf.lastApplied, rf.commitIndex)
		rf.cond.Broadcast()
	}

	rf.applying = false
}
