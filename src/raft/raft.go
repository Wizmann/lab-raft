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

import "sync"
import "labrpc"

// import "bytes"
// import "labgob"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex          // Lock to protect shared access to this peer's state
    peers     []*labrpc.ClientEnd // RPC end points of all peers
    persister *Persister          // Object to hold this peer's persisted state
    me        int                 // this peer's index into peers[]

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    LeaderId int
    currentTerm int
    votedFor int
    voteACK int
    state    NodeState
    running bool
    exit bool

    // log[]

    commitIndex int
    lastApplied int

    timer *time.Timer

    heartbeat_timeout time.Duration
    election_timeout time.Duration


}

// return currentTerm and whether this server
// believes it is the leader.
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    Term int
    VoteGranted bool

    VoteFrom int // for log
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if (rf.state == LEADER) {
        return
    }

    if (args.Term > rf.currentTerm) {
        rf.currentTerm = args.Term
        rf.votedFor = args.CandidateId

        reply.Term = args.Term
        reply.VoteGranted = true
        reply.VoteFrom = rf.me

        rf.updateStateTo(FOLLOWER)
    } else {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        reply.VoteFrom = rf.me
    }
}

// not thread-safe, add concurrency control when call this func
func (rf *Raft) updateStateTo(newState NodeState) {
    DPrintf("Node[%d] updateState [%s] to [%s]",
        rf.me, rf.state.ToString(), newState.ToString())
    rf.state = newState
    switch newState {
    case LEADER:
        rf.renewTimer(rf.heartbeat_timeout)
    case CANDIDATE:
        rf.renewTimer(rf.getRandomRestartElectionTimeout())
    case FOLLOWER:
        rf.renewTimer(rf.election_timeout)
    }
}

func (rf *Raft) sendAppendEntries(args *AppendEntriesArgs) {
    for i, _ := range rf.peers {
        if (i == rf.me) {
            continue
        }
        go func (server int) {
            var reply AppendEntriesReply
            rf.peers[server].Call("Raft.AppendEntries", args, &reply)
            if (!reply.Success) {
                if (reply.Term > rf.currentTerm) {
                    rf.mu.Lock()
                    defer rf.mu.Unlock()

                    rf.updateStateTo(FOLLOWER)
                }
            }
        }(i)
    }
}

func (rf *Raft) sendHeartbeat() {
    DPrintf("Node[%d] send leader heartbeat", rf.me)
    args := AppendEntriesArgs {
        Term: rf.currentTerm,
        LeaderId: rf.me,
    }
    rf.sendAppendEntries(&args)
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
        rf.commitIndex = args.LeaderCommit // really?
        rf.LeaderId = args.LeaderId

        switch (rf.state) {
        case LEADER:
            rf.updateStateTo(FOLLOWER)
        case FOLLOWER:
            // pass
        case CANDIDATE:
            rf.updateStateTo(FOLLOWER)
        }

        reply.Success = false
    } else {
        if (rf.state == LEADER || rf.state == CANDIDATE) {
            rf.updateStateTo(FOLLOWER);
        }

        rf.LeaderId = args.LeaderId

        if (args.LeaderCommit > rf.commitIndex) {
            rf.commitIndex = args.LeaderCommit // really?
        }
        DPrintf("Node[%d] get heartbeat from node %d", rf.me, args.LeaderId)
        reply.Success = true
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
    index := -1
    term := -1
    isLeader := true

    // Your code here (2B).


    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    /*
    rf.running = false
    for !rf.exit {
        time.Sleep(1000 * time.Millisecond);
    }
    time.Sleep(1000 * time.Millisecond);
    */
}

func (rf *Raft) startElection() {
    rf.LeaderId = -1
    rf.votedFor = -1
    rf.voteACK = 0
    rf.updateStateTo(CANDIDATE)
}

func (rf *Raft) restartElection() {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if (rf.state != CANDIDATE) {
        return
    }

    DPrintf("Node[%d] election timeout", rf.me);

    rf.LeaderId = -1
    rf.votedFor = rf.me
    rf.voteACK = 1
    rf.currentTerm += 1

    currentTerm := rf.currentTerm

    for i, _ := range rf.peers {
        if (i == rf.me) {
            continue;
        }

        go func(server int, currentTerm int) {
            if (rf.currentTerm != currentTerm) {
                return;
            }

            args := RequestVoteArgs {
                Term : currentTerm,
                CandidateId : rf.me,
            }

            var reply RequestVoteReply
            rf.sendRequestVote(server, &args, &reply)

            rf.mu.Lock()
            defer rf.mu.Unlock()
            if (rf.state == CANDIDATE && reply.VoteGranted && reply.Term == currentTerm) {
                DPrintf("Node[%d] get voteACK from node %d", rf.me, reply.VoteFrom)
                rf.voteACK += 1
                if (rf.voteACK * 2 > len(rf.peers)) {
                    rf.sendHeartbeat()
                    rf.updateStateTo(LEADER)
                    rf.LeaderId = rf.me
                }
            } else if (reply.Term > currentTerm) {
                rf.updateStateTo(FOLLOWER)
            }
        }(i, currentTerm)
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
        DPrintf("Node[%d] renew timer %d ms", rf.me, timeout / 1000000)
    }
}


func (rf *Raft) process() {
    DPrintf("Node[%d] is processing, state: [%s], term %d, leader %d", 
            rf.me, rf.state.ToString(), rf.currentTerm, rf.LeaderId);
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

    rf.heartbeat_timeout = time.Duration(200 * time.Millisecond)
    rf.election_timeout = time.Duration(500 * time.Millisecond)

    // notify state changed message to the main loop
    // no need to block the channel

    rf.updateStateTo(FOLLOWER)
    rf.running = true;
    rf.exit = false

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    go func() {
        for rf.running {
            DPrintf("Node[%d] is %s, term %d, leader %d",
                rf.me, rf.state.ToString(), rf.currentTerm, rf.LeaderId)
            select {
            case <- rf.timer.C:
                rf.process()
            }
        }
        rf.exit = true
    }()





    return rf
}
