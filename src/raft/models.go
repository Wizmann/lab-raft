package raft

import (
    "fmt"
)

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

type AppendEntriesArgs struct {
    Term     int
    LeaderId int

    PrevLogIndex int
    PrevLogTerm  int

    // entries[]
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term int
    Success bool
    LeaderId int

    PrevLogIndex int
    PrevLogTerm int

    LeaderCommit int
}
