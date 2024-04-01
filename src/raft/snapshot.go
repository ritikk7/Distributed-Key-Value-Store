package raft

import "time"

func (rf *Raft) GetAbsoluteIndex(relativeIndex int) int {
	return relativeIndex + (rf.lastIncludedIndex + 1)
}

func (rf *Raft) GetRelativeIndex(absoluteIndex int) int {
	return absoluteIndex - (rf.lastIncludedIndex + 1)
}

func (rf *Raft) sendSnapshots(pId int, term int) {
	if pId == rf.me {
		return
	}

	for !rf.killed() {
		rf.mu.Lock()

		if rf.raftState != Leader || rf.currTerm > term {
			Debugf(dTerm, "S%v stopped sendSnapshot, state %v != leader OR currTerm %v > initialTerm %v\n", rf.me, rf.raftState, rf.currTerm, term)
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()

		rf.snapshotCond[pId].L.Lock()
		for rf.matchIndex[pId] >= rf.GetAbsoluteIndex(len(rf.log)-1) {
			rf.snapshotCond[pId].Wait()
			continue
		}
		rf.snapshotCond[pId].L.Unlock()

		rf.mu.Lock()

		leaderId := rf.me
		lastIncludedIndex := rf.lastIncludedIndex
		lastIncludedTerm := rf.lastIncludedTerm
		snapshot := rf.persister.ReadSnapshot()

		matchIdx := rf.lastIncludedIndex
		nextIdx := lastIncludedIndex + 1

		Debugf(dSnap, "S%v -> S%v, sending snapshot, nextIdx %v, snapshotTerm %v [%v]\n", rf.me, pId, rf.nextIndex[pId], lastIncludedTerm, lastIncludedIndex)
		rf.mu.Unlock()

		args := InstallSnapshotArg{
			Term:              term,
			LeaderId:          leaderId,
			LastIncludedIndex: lastIncludedIndex,
			LastIncludedTerm:  lastIncludedTerm,
			Data:              snapshot,
		}

		reply := InstallSnapShotReply{}

		ok := rf.peers[pId].Call(INSTALL_SNAPSHOT_RPC, &args, &reply)

		if !ok {
			rf.mu.Lock()
			Debugf(dError, "S%v -> S%v, sendSnapshot failed, no connection, term %v\n", rf.me, pId, rf.currTerm)
			rf.mu.Unlock()
			continue
		}

		rf.mu.Lock()

		if rf.raftState != Leader {
			rf.mu.Unlock()
			return
		}

		if rf.currTerm > term {
			Debugf(dTerm, "S%v -> S%v, sendSnapshot reply, currTerm %v> argsTerm %v, current call outdated, stopping\n", rf.me, pId, rf.currTerm, term)
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.currTerm {
			rf.raftState = Follower
			rf.currTerm = reply.Term
			rf.persistState()
			Debugf(dTerm, "S%v -> S%v, sendSnapshot reply, replyTerm %v> currTerm %v, stopping\n", rf.me, pId, reply.Term, rf.currTerm)
			rf.mu.Unlock()
			return
		}

		if reply.LastIncludedIndex > lastIncludedIndex {
			rf.mu.Unlock()
			continue
		}

		rf.matchIndex[pId] = matchIdx
		rf.nextIndex[pId] = nextIdx

		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
