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
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

type Config struct {
	heartbeatInterval   int64 //heartbeat interval in millisecond
	timeoutIntervalMin  int64
	timeoutIntervalDiff int64
}

var defaultConfig = &Config{
	heartbeatInterval:   150,
	timeoutIntervalMin:  300,
	timeoutIntervalDiff: 150,
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int

	role   int //role state of the server: 0 for follower, 1 for candidate, 2 for leader
	roleMu sync.Mutex

	votedFor int //index for voted for?

	commitIndex  int //last index known to be committed
	appliedIndex int //last index known to be applied to SM

	lastLogIndex int
	lastLogTerm  int

	heartBeatReceived int32 //indicator for whether heartbeat is received from leader, reset to 0 on ticker, set to 1 on AppendEntriesHandler when receiving from valid leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.role == 2
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

// term change func, if there's an increment in the current term, should clear votedFor for the new current term.
func (rf *Raft) incrementTerm(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if newTerm <= rf.currentTerm {
		return
	}
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

// start an election, switch to candidate state and request vote from all peers
func (rf *Raft) becomeCandidate() {
	rf.resetTimeoutTicker()
	rf.currentTerm += 1

	fmt.Printf("%d started election for term %d \n", rf.me, rf.currentTerm)

	rf.role = 1
	reqVoteArgs := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastLogIndex, LastLogTerm: rf.lastLogTerm}
	voteRec := 1                          //vote for self
	voteNeeded := (len(rf.peers) / 2) + 1 //majority count num
	voteMu := sync.Mutex{}
	rf.votedFor = rf.me
	for i := 0; i < len(rf.peers); i++ { //request vote from peer
		if i == rf.me { //ignore self
			continue
		}
		go func(index int) { //go routine to request vote and handle result
			reply := &RequestVoteReply{}
			gotResponse := false
			for !gotResponse { //if RPC not successful, call again
				gotResponse = rf.sendRequestVote(index, reqVoteArgs, reply)
			}
			if rf.role != 1 { //if not candidate anymore, return
				return
			} // not a candidate anymore
			if reply.VoteGranted { //received a vote
				voteMu.Lock()
				defer voteMu.Unlock()
				voteRec += 1
				fmt.Printf("%d election term %d got vote from %d, %d/%d\n", rf.me, rf.currentTerm, index, voteRec, voteNeeded)
				if voteRec == voteNeeded { //become leader
					go rf.becomeLeader()
					return
				}
			} else { //didn't get vote
				if reply.Term > rf.currentTerm {
					rf.becomeFollower()
				}
				return
			}
		}(i)
	}
}

// start as new leader for current term
func (rf *Raft) becomeLeader() {
	if rf.role == 2 { //already a leader, exit
		return
	}
	rf.roleMu.Lock()
	defer rf.roleMu.Unlock()
	rf.role = 2 //change to leader role
	fmt.Printf("%d I'm a leader for term %d\n", rf.me, rf.currentTerm)
	//heartbeat func to all peers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		} //ignore self
		go rf.sendHeartBeat(i, rf.currentTerm) //start the send heartbeat func to peers
	}
}

func (rf *Raft) becomeFollower() {
	rf.roleMu.Lock()
	rf.role = 0
	rf.roleMu.Unlock()
}

// repeatedly send heartbeat to peer, stop when cur term change or cur role no longer a leader
func (rf *Raft) sendHeartBeat(peerIndex int, term int) {
	heartBeatRPCArgs := &AppendEntriesArgs{Term: term}
	for {
		if term != rf.currentTerm || rf.role != 2 { //if term expires or no longer leader, return
			return
		}
		rf.sendAppendEntries(peerIndex, heartBeatRPCArgs, &AppendEntriesReply{})
		time.Sleep(time.Duration(defaultConfig.heartbeatInterval) * time.Millisecond)
	}
}

// /***REQUEST_VOTE***///
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int //candidates term
	CandidateId  int //candidateId
	LastLogIndex int //
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  //highest term of the receiver
	VoteGranted bool //whether voted for caller
}

// request vote handler for a server.
// 1. Reply false if args.term < currentTerm
// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false //set to not vote initially
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.incrementTerm(args.Term) // update the term to sender's term, indicating a more recent election
	if rf.votedFor != -1 {      //already voted for other in cur term
		return
	}
	if rf.lastLogTerm > args.LastLogTerm { //cur log term larger, should deny
		return
	}
	if rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex { //same log term but cur log index bigger, should deny
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.becomeFollower()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	return ok
}

// /***AppendEntriesRPC***///
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //index of log entry immediately preceding new ones
	PrevLogTerm  int //term of prevLogIndex entry
	//entries[]
	LeaderCommit int //leader's commitIndex

}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { //invalid sender
		reply.Success = false
		return
	}
	rf.incrementTerm(args.Term)
	rf.resetTimeoutTicker()

	reply.Success = true
	rf.roleMu.Lock()
	defer rf.roleMu.Unlock()
	if rf.role != 0 { //if not a follower, change to follower
		rf.role = 0
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	//check if I'm still leader
	if reply.Term > rf.currentTerm {
		rf.becomeFollower()
	}
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
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.role != 2 && atomic.LoadInt32(&rf.heartBeatReceived) != 1 { //start election
			rf.becomeCandidate()
		}

		atomic.StoreInt32(&rf.heartBeatReceived, 0) //reset heartbeat indicator
		// pause for a random amount of time between 250 and 550
		// milliseconds.
		ms := defaultConfig.timeoutIntervalMin + (rand.Int63() % defaultConfig.timeoutIntervalDiff)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) resetTimeoutTicker() {
	atomic.StoreInt32(&rf.heartBeatReceived, 1)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.role = 0
	rf.votedFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
