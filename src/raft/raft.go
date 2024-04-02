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
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
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
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh  chan ApplyMsg
	voteCh   chan bool
	appendCh chan bool
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 3A
	state State

	// Persistent state
	currentTerm int        // 当前任期号
	votedFor    int        // 该 Term 中已接收到该节点的选票的候选人id
	log         []LogEntry // 日志

	// all-server volatile states
	commitIndex int // 最后一个已提交日志记录的索引
	lastApplied int // 最后一个已应用至上层状态机的日志记录的索引

	// leader volatile states
	nextIndex  []int // 每个服务器，要为其发送的下一个日志索引
	matchIndex []int // 每个服务器，已备份的最后一条日志记录的索引
}

type AppendEntriesArgs struct {
	Term         int        // 领导者的任期号
	LeaderId     int        // 领导者id
	PrevLogIndex int        // 最新日志之前日志的索引
	PrevLogTerm  int        // 最新日志之前日志的任期号
	Entries      []LogEntry // 要追加的日志条目
	LeaderCommit int        // 领导者的已提交日志索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期号
	Success bool // 是否成功

	// fast backup
	Xterm int // 与领导者冲突的任期号
	Xindex int // 与领导者冲突的日志索引
	Xlen int // 空白的日志长度
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
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
	// Your code here (3C).
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
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 任期号
	CandidateId  int // 候选人id
	LastLogIndex int // 候选人最新日志的索引
	LastLogTerm  int // 候选人最新日志的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期号
	VoteGranted bool // 是否投票
}

func sendCh(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIndex()
	return rf.log[idx].Term
}

func (rf *Raft) getPrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

func (rf *Raft) getPrevLogTerm(server int) int {
	idx := rf.getPrevLogIndex(server)
	if idx < 0 {
		return -1
	}
	return rf.log[idx].Term
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		return
	}
	// 很重要的一步，可以在一定程度上避免多个节点同时选举
	if args.Term > rf.currentTerm {
		rf.followLeader(args.Term)
		sendCh(rf.voteCh)
	}
	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			sendCh(rf.voteCh)
			log.Printf("%d server send vote to %d term in RequestVote\n", rf.me, args.Term)
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%d server %d term receive heartbeat from %d server %d term\n", rf.me, rf.currentTerm, args.LeaderId ,args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.followLeader(args.Term)
	}
	sendCh(rf.appendCh)
	reply.Term = rf.currentTerm
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	reply.Success = true
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitTo(Min(args.LeaderCommit, len(rf.log)-1))
	}
	// unmatch_idx := -1
    // for idx := range args.Entries {
    //     if len(rf.log) < (args.PrevLogIndex+2+idx) ||
    //         rf.log[(args.PrevLogIndex+1+idx)].Term != args.Entries[idx].Term {
    //         // unmatch log found
    //         unmatch_idx = idx
    //         break
    //     }
    // }

    // if unmatch_idx != -1 {
    //     // there are unmatch entries
    //     // truncate unmatch Follower entries, and apply Leader entries
    //     rf.log = rf.log[:(args.PrevLogIndex + 1 + unmatch_idx)]
    //     rf.log = append(rf.log, args.Entries[unmatch_idx:]...)
    // }

	log.Printf("%d becomes follwer in AppendEntries\n", rf.me)
	rf.followLeader(args.Term)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == Leader)
	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
	}
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
	heartBeat := time.Duration(100) * time.Millisecond
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		// 稍微大一点，不然在appendlog之前就超时了
		electionTime := time.Duration(150+(rand.Int63()%250)) * time.Millisecond
		log.Printf("%d server %v time\n", rf.me, electionTime)
		switch state {
		case Follower:
			select {
			case <-rf.voteCh:
			case <-rf.appendCh:
			case <-time.After(electionTime):
				log.Printf("Server %d is becoming candidate\n", rf.me)
				rf.newElection()
			}
		case Candidate:
			select {
			case <-rf.voteCh:
			case <-rf.appendCh:
			case <-time.After(electionTime):
				log.Printf("Candidate %d starts new selection\n", rf.me)
				rf.newElection()
			}
		case Leader:
			log.Printf("Leader %d is sending heartbeats\n", rf.me)
			rf.newAppendLog()
			time.Sleep(heartBeat)
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// 3A
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0

	rf.applyCh = applyCh
	rf.voteCh = make(chan bool, 1)
	rf.appendCh = make(chan bool, 1)

	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	logFile, _ := os.OpenFile("log.txt"+fmt.Sprintf("%d", testcnt), os.O_RDWR|os.O_CREATE, 0766)
	// logFile, _ := os.OpenFile("/dev/null", os.O_RDWR|os.O_CREATE, 0766)
	log.SetOutput(logFile)
	log.SetFlags(log.Lmicroseconds)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) newElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.mu.Unlock()

	votes := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.state == Follower {
			return
		}
		go func(idx int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.getLastLogTerm(),
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(idx, &args, &reply)
			log.Printf("Server %d request vote from %d server\n", rf.me, idx)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.followLeader(reply.Term)
					sendCh(rf.voteCh)
					return
				}
				if reply.VoteGranted {
					votes++
				}
				if votes > len(rf.peers)/2 && rf.state == Candidate {
					rf.becomeLeader()
					sendCh(rf.voteCh)
					log.Printf("%d becomes leader\n", rf.me)
				}
			}
		}(i)
	}
}

func (rf *Raft) newAppendLog() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		go func(idx int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.getPrevLogIndex(idx),
				PrevLogTerm:  rf.getPrevLogTerm(idx),
				Entries:      rf.log[rf.getPrevLogIndex(idx)+1:],
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(idx, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if reply.Term > rf.currentTerm {
					rf.followLeader(reply.Term)
					return
				}
				if reply.Success {
					rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[idx] = rf.matchIndex[idx] + 1
					for N := (len(rf.log) - 1); N > rf.commitIndex; N-- {
						count := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= N {
								count += 1
							}
						}
						if count > len(rf.peers)/2 {
							rf.commitTo(N)
							break
						}
					}
				} else {
					rf.nextIndex[idx]--
				}
			}
		}(i)
	}

}

func (rf *Raft) followLeader(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) becomeLeader() {
	if rf.state != Candidate{
		return
	}
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
}

func (rf*Raft) commitTo(commitIndex int){
	rf.commitIndex = commitIndex
	if rf.commitIndex > rf.lastApplied{
		entries := append([]LogEntry{}, rf.log[(rf.lastApplied+1):(rf.commitIndex+1)]...)
		go func(last int, Entries []LogEntry){
			for idx, entry := range Entries{
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: last+idx}
				rf.mu.Lock()
				rf.lastApplied++
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, entries)
	}
}
