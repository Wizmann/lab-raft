package raft

import (
    _ "fmt"
    "time"
    "math/rand"
    _ "log"
)


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

import "labrpc"

// import "bytes"
// import "labgob"

func (rf *Raft) GetState() (int, bool) {
    term := rf.currentTerm
    isleader := rf.state == LEADER

    return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    // Your code here (2C).
    // Example:
    // w := new(bytes.Buffer)
    // e := labgob.NewEncoder(w)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // data := w.Bytes()
    // rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if (rf.state == LEADER) {
        return
    }

    DPrintf("Node[%d] RequestVote from Node[%d] [%d,%d,%d], current [%d,%d,%d]",
            rf.me, args.CandidateId,
            args.Term, args.LastLogTerm, args.LastLogIndex,
            rf.currentTerm, rf.GetLastLogTerm(), rf.GetLastLogIndex())

    reply.VoteGranted = false

    if (args.Term > rf.currentTerm) {
        if (args.LastLogTerm > rf.GetLastLogTerm() ||
                args.LastLogTerm == rf.GetLastLogTerm() && args.LastLogIndex >= rf.GetLastLogIndex()) {
            rf.votedFor = args.CandidateId

            reply.Term = rf.currentTerm
            reply.VoteGranted = true
            reply.VoteFrom = rf.me

            rf.updateStateTo(FOLLOWER)
        }
        rf.currentTerm = args.Term
    }

    reply.Term = rf.currentTerm
    reply.VoteFrom = rf.me
    return
}

// not thread-safe, add concurrency control when call this func
func (rf *Raft) updateStateTo(newState NodeState) {
    DPrintf("Node[%d] updateState [%s] to [%s]",
        rf.me, rf.state.ToString(), newState.ToString())
    rf.state = newState
    switch newState {
    case LEADER:
        // init leader log pointers
        for i := 0; i < len(rf.peers); i++ {
            rf.NextIndex[i] = rf.GetLastLogIndex() + 1;
            rf.MatchIndex[i] = rf.GetLastLogIndex();
        }
        rf.sendHeartbeat()
        rf.renewTimer(rf.heartbeat_timeout)
    case CANDIDATE:
        rf.renewTimer(rf.getRandomRestartElectionTimeout())
    case FOLLOWER:
        rf.renewTimer(rf.election_timeout)
    }
}

func (rf *Raft) sendAppendEntries(logIndex int) {
    for i, _ := range rf.peers {
        if (i == rf.me) {
            continue
        }

        if (rf.state != LEADER) {
            return;
        }

        go func (server int) {
            rf.sendAppendEntriesToNode(server, logIndex);
        }(i)
    }
}

func (rf *Raft) sendAppendEntriesToNode(nodeId int, logIndex int) {
    if (rf.state != LEADER) {
        return;
    }

    args := func() AppendEntriesArgs {
        rf.mu.Lock();
        defer rf.mu.Unlock()

        args := AppendEntriesArgs {
            LeaderId: rf.me,
            LeaderCommit: rf.lastApplied,
        }

        // heartbeat
        if (logIndex == -1) {
            args.Term = rf.currentTerm;
            args.PrevLogTerm = rf.GetLastLogTerm();
            args.PrevLogIndex = rf.GetLastLogIndex();
            Assert(len(args.Entries) == 0, "Node[%d] heartbeat should not have entries")
        } else {
            args.Term = rf.Entries[logIndex].Term;
            args.PrevLogTerm = rf.Entries[logIndex - 1].Term;
            args.PrevLogIndex = rf.Entries[logIndex - 1].Index;
            args.Entries = append(args.Entries, rf.Entries[logIndex]);
        }

        return args
    }()

    var reply AppendEntriesReply
    ok := false
    ok = rf.peers[nodeId].Call("Raft.AppendEntries", &args, &reply)

    DPrintf("Node[%d] send AppendEntries RPC to node %d, logIndex: %d",
            rf.me, nodeId, logIndex);

    // it could be OK if the RPC call is not success
    // the next round of leader heartbeat will detect the missing log and try to resend it again
    rf.mu.Lock();
    defer rf.mu.Unlock()
    for ok {
        if (rf.state != LEADER) {
            break;
        }

        if (reply.Term > rf.currentTerm) {
            DPrintf("Node[%d] reply.Term=%d, rf.currentTerm=%d", rf.me, reply.Term, rf.currentTerm);
            rf.updateStateTo(FOLLOWER)
            break;
        }

        rollback := false

        if (len(reply.Entries) > 0) {
            n := len(reply.Entries);
            for i := 0; i < n; i++ {
                logEntry := reply.Entries[i]
                logIndex := logEntry.Index;

                Assert(rf.Entries[logIndex].LogId == logEntry.LogId,
                        "Node[%d] has unmatched log with node %d on index[%d], %s != %s",
                        rf.me, nodeId, logIndex, rf.Entries[logIndex].LogId, logEntry.LogId);

                if (logIndex == rf.NextIndex[nodeId]) {
                    rf.NextIndex[nodeId] += 1;
                }

                rf.leaderTryApplyLog(nodeId, logIndex);
            }
        } else if (!reply.Success) {
            DPrintf("Node[%d] AppendEntries to node %d failed. reply prevLog [%d,%d]", 
                    rf.me, nodeId, reply.PrevLogTerm, reply.PrevLogIndex);
            rf.NextIndex[nodeId] = Min(rf.NextIndex[nodeId], reply.PrevLogIndex + 1);
            rollback = true;
        }

        if (rf.NextIndex[nodeId] == rf.commitIndex + 1) {
            break;
        }

        Assert(rf.NextIndex[nodeId] > 0,
                "Node[%d] next index of node %d should always greater than 0",
                rf.me, nodeId);

        args := AppendEntriesArgs {
            Term: rf.currentTerm,
            LeaderId: rf.me,
            PrevLogTerm:  rf.Entries[rf.NextIndex[nodeId] - 1].Term,
            PrevLogIndex: rf.Entries[rf.NextIndex[nodeId] - 1].Index,
            LeaderCommit: rf.lastApplied,
        }

        cnt := 1
        if (rollback == false) {
            cnt = rf.NextIndex[nodeId] - reply.PrevLogIndex;
        }

        for i := 0; i < cnt; i++ {
            if (rf.NextIndex[nodeId] + i > rf.commitIndex) {
                break;
            }
            args.Entries = append(args.Entries, rf.Entries[rf.NextIndex[nodeId] + i]);
        }

        reply = AppendEntriesReply{}
        ok = rf.peers[nodeId].Call("Raft.AppendEntries", &args, &reply)
    }
}

func (rf *Raft) leaderTryApplyLog(nodeId int, logIndex int) {
    Assert(rf.state == LEADER, "Node[%d] is not leader", rf.me);
    if (rf.MatchIndex[nodeId] < logIndex) {
        rf.MatchIndex[nodeId] = logIndex;
    }

    if (rf.Entries[logIndex].Term < rf.currentTerm) {
        return;
    }

    minIndex := -1
    minIndexCount := 0
    for i := 0; i < len(rf.peers); i++ {
        if (i == rf.me) {
            continue;
        }
        if (minIndex == -1 || rf.MatchIndex[i] > minIndex) {
            minIndex = rf.MatchIndex[i];
            minIndexCount = 1;
        } else if (rf.MatchIndex[i] == minIndex) {
            minIndexCount++;
        }
    }

    if ((minIndexCount + 1) * 2 > len(rf.peers)) {
        for rf.lastApplied + 1 <= minIndex {
            rf.applyMessage(rf.Entries[rf.lastApplied + 1]);
            rf.lastApplied++;
        }
        rf.lastApplied = minIndex;
    }
}

func (rf *Raft) followerTryApplyLog(leaderCommit int) {
    Assert(rf.state == FOLLOWER, "Node[%d] is not follower", rf.me);
    applyTo := leaderCommit
    if (rf.commitIndex <= applyTo) {
        applyTo = rf.commitIndex;
    }
    for i := rf.lastApplied + 1; i <= applyTo; i++ {
        rf.applyMessage(rf.Entries[i]);
    }
    rf.lastApplied = applyTo;
}

func (rf *Raft) applyMessage(entry LogEntry) {
    msg := ApplyMsg {
        CommandIndex: entry.Index,
        CommandValid: true,
        Command: entry.Command,
    };

     rf.applyCh <- msg;
}

func (rf *Raft) sendHeartbeat() {
    rf.sendAppendEntries(-1)
    rf.renewTimer(rf.heartbeat_timeout)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if (args.Term < rf.currentTerm) {
        DPrintf("Node[%d] AppendEntries failed, term expected[%d], actual[%d]",
            rf.me, rf.currentTerm, args.Term)

        reply.Success = false
        reply.Term = rf.currentTerm
    } else if (args.Term > rf.currentTerm) {
        DPrintf("Node[%d] AppendEntries failed, term expected[%d], actual[%d]",
            rf.me, rf.currentTerm, args.Term)

        rf.currentTerm = args.Term
        rf.LeaderId = args.LeaderId

        switch (rf.state) {
        case LEADER:
            rf.updateStateTo(FOLLOWER)
        case FOLLOWER:
            // pass
        case CANDIDATE:
            rf.updateStateTo(FOLLOWER)
        }

        reply.Success = true;
        reply.PrevLogTerm = rf.GetLastLogTerm();
        reply.PrevLogIndex = rf.GetLastLogIndex();
    } else {
        if (rf.state == LEADER || rf.state == CANDIDATE) {
            rf.updateStateTo(FOLLOWER);
        }

        DPrintf("Node[%d] get AppendEntries RPC from Node[%d], LeaderCommit:%d, LastApplied:%d",
            rf.me, args.LeaderId, args.LeaderCommit, rf.lastApplied);

        if (args.PrevLogTerm != rf.GetLastLogTerm() || args.PrevLogIndex != rf.GetLastLogIndex()) {
            DPrintf("Node[%d] get wrong message from node %d, args log status [%d,%d], rf log status[%d,%d]", 
                    rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex,
                    rf.GetLastLogTerm(), rf.GetLastLogIndex());

            Assert(rf.commitIndex == len(rf.Entries) - 1,
                    "Node[%d] get log entries index is not match, commitIndex: %d, entries num: %d (without dummy)",
                    rf.me, rf.commitIndex, len(rf.Entries) - 1)

            if (rf.GetLastLogIndex() > args.PrevLogIndex) {
                DPrintf("Node[%d] rollback commitIndex from %d to %d", rf.me, rf.commitIndex, args.PrevLogIndex);
                rf.commitIndex = args.PrevLogIndex
                rf.Entries = rf.Entries[:rf.commitIndex + 1]
            } else if (rf.GetLastLogIndex() == args.PrevLogIndex) {
                DPrintf("Node[%d] rollback commitIndex from %d to %d", rf.me, rf.commitIndex, rf.commitIndex - 1);
                rf.commitIndex -= 1
                rf.Entries = rf.Entries[:len(rf.Entries) - 1]
            }

            Assert(rf.commitIndex == len(rf.Entries) - 1,
                    "Node[%d] has mismatched log index, commitIndex: %d, number of entries: %d(without dummy)",
                    rf.me, rf.commitIndex, len(rf.Entries) - 1);
            Assert(rf.GetLastLogIndex() >= 0, "Node[%d] log index should at least equal to 0", rf.me);

            reply.Success = false;
            reply.PrevLogTerm = rf.GetLastLogTerm();
            reply.PrevLogIndex = rf.GetLastLogIndex();
        } else {
            for _, entry := range args.Entries {
                rf.Entries = append(rf.Entries, entry)
                rf.commitIndex += 1;
                Assert(rf.commitIndex == entry.Index,
                        "Node[%d] log index should be continuous. rf.commitIndex[%d] != entry.Index[%d]",
                        rf.me, rf.commitIndex, entry.Index);
                DPrintf("Node[%d] append log entry [%d,%d,%x,%s]",
                        rf.me, entry.Term, entry.Index, entry.Command.(int), entry.LogId);
                reply.Entries = append(reply.Entries, entry)
            }
            rf.followerTryApplyLog(args.LeaderCommit);
            reply.Success = true
            reply.PrevLogTerm = rf.GetLastLogTerm();
            reply.PrevLogIndex = rf.GetLastLogIndex();
        }

        rf.renewTimer(rf.election_timeout)
    }
}


//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    index := -1;
    term := -1;
    isLeader := rf.state == LEADER;

    if (isLeader == false) {
        return index, term, isLeader
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()

    term = rf.currentTerm
    index = rf.commitIndex + 1
    rf.commitIndex += 1

    entry := LogEntry {
        Term: term,
        Index: index,
        LogId: CreateLogId(),
        Command: command,
    }

    rf.Entries = append(rf.Entries, entry)

    rf.NextIndex[rf.me] += 1

    DPrintf("Node[%d] Start sending commands (%d,%d,%x,%s)",
            rf.me, entry.Term, entry.Index, entry.Command.(int), entry.LogId);

    rf.sendAppendEntries(entry.Index);

    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    // pass
}

func (rf *Raft) GetLastLogTerm() int {
    return rf.Entries[rf.commitIndex].Term;
}

func (rf *Raft) GetLastLogIndex() int {
    return rf.Entries[rf.commitIndex].Index;
}

func (rf *Raft) GetLastLogEntry() LogEntry {
    return rf.Entries[rf.commitIndex];
}

func (rf *Raft) startElection() {
    rf.LeaderId = -1
    rf.votedFor = -1
    rf.voteACK = 0
    rf.updateStateTo(CANDIDATE)
}

func (rf *Raft) restartElection() {
    DPrintf("Node[%d] election timeout", rf.me);
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if (rf.state != CANDIDATE) {
        return;
    }

    rf.LeaderId = -1
    rf.votedFor = rf.me
    rf.voteACK = 1

    rf.currentTerm += 1;

    for i, _ := range rf.peers {
        if (i == rf.me) {
            continue;
        }

        go func(server int, currentTerm int, lastLogTerm int, lastLogIndex int) {
            args := RequestVoteArgs {
                Term : currentTerm,
                CandidateId : rf.me,
                LastLogTerm: lastLogTerm,
                LastLogIndex: lastLogIndex,
            }

            var reply RequestVoteReply
            rf.sendRequestVote(server, &args, &reply)

            rf.mu.Lock()
            defer rf.mu.Unlock()

            if (rf.state == CANDIDATE && reply.VoteGranted && reply.Term == rf.currentTerm) {
                DPrintf("Node[%d] get voteACK from node %d", rf.me, reply.VoteFrom)
                rf.voteACK += 1
                if (rf.voteACK * 2 > len(rf.peers)) {
                    rf.updateStateTo(LEADER)
                    rf.LeaderId = rf.me
                    rf.currentTerm = reply.Term
                }
            } else if (rf.state == LEADER && reply.Term > rf.currentTerm) {
                rf.currentTerm = reply.Term
                rf.updateStateTo(FOLLOWER)
            }
        }(i, rf.currentTerm, rf.GetLastLogTerm(), rf.GetLastLogIndex())
    }
    rf.renewTimer(rf.getRandomRestartElectionTimeout())
}

func (rf *Raft) getRandomRestartElectionTimeout() time.Duration {
    const l = 500
    const r = 1000
    return time.Millisecond * time.Duration(l + rand.Int63n(r - l))
}


func (rf *Raft) renewTimer(timeout time.Duration) {
    if (rf.timer == nil) {
        rf.timer = time.NewTimer(timeout)
    } else {
        if (!rf.timer.Stop()) {
            select {
            case <- rf.timer.C:
                // pass
            default:
                // pass
            }
        }
        rf.timer.Reset(timeout)
    }
}


func (rf *Raft) process() {
    DPrintf("Node[%d] is processing, state: [%s], term %d, commitIndex: %d, lastApplied: %d, leader %d", 
            rf.me, rf.state.ToString(), rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.LeaderId);
    switch rf.state {
    case LEADER:
        rf.sendHeartbeat()
    case FOLLOWER:
        rf.startElection()
    case CANDIDATE:
        rf.restartElection()
    }
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    // Your initialization code here (2A, 2B, 2C).
    rf.LeaderId = -1
    rf.currentTerm = 0
    rf.commitIndex = 0
    rf.lastApplied = 0
    rf.state = FOLLOWER
    rf.votedFor = -1
    rf.voteACK = 0
    rf.applyCh = applyCh

    dummyEntry := LogEntry{};
    // a magic string
    dummyEntry.LogId = "9fb0f039-465a-4a71-a402-7e045103da6e";

    rf.Entries = append(rf.Entries, dummyEntry);

    rf.heartbeat_timeout = time.Duration(300 * time.Millisecond)
    rf.election_timeout = time.Duration(600 * time.Millisecond)

    rf.NextIndex = make([]int, len(rf.peers));
    rf.MatchIndex = make([]int, len(rf.peers));

    rf.updateStateTo(FOLLOWER);


    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    go func() {
        for {
            DPrintf("Node[%d] is %s processing, term %d, leader %d",
                rf.me, rf.state.ToString(), rf.currentTerm, rf.LeaderId)
            select {
            case <- rf.timer.C:
                rf.process()
            }
        }
    }()

    return rf
}
