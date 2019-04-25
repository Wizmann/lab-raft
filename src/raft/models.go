package raft

import (
    "fmt"
    "sync"
    "labrpc"
    "time"
)

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

    Entries []LogEntry

    commitIndex int // log to be committed
    lastApplied int // log have been committed to the state machine

    timer *time.Timer

    heartbeat_timeout time.Duration
    election_timeout time.Duration


    applyCh chan ApplyMsg
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int
}

type NodeState int

const (
    UNKNOWN   NodeState = 0
    LEADER    NodeState = 1
    CANDIDATE NodeState = 2
    FOLLOWER  NodeState = 3
)

func (state NodeState) ToString() string {
    switch state {
    case UNKNOWN:
        return "UNKNOWN"
    case LEADER:
        return "LEADER"
    case CANDIDATE:
        return "CANDIDATE"
    case FOLLOWER:
        return "FOLLOWER"
    default:
        return fmt.Sprintf("error state: %d", int(state))
    }
}

type LogEntry struct {
    Term    int
    Index   int
    AckCount int
    Command interface{}
}

type AppendEntriesArgs struct {
    LeaderId int

    Term     int

    PrevLogTerm  int
    PrevLogIndex int

    Entries []LogEntry

    LeaderCommit int
}

type AppendEntriesReply struct {
    Term int
    Success bool
    LeaderId int

    PrevLogTerm int
    PrevLogIndex int

    Entries []LogEntry

    LeaderCommit int
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

type RequestVoteReply struct {
    Term int
    VoteGranted bool

    VoteFrom int // for log
}
