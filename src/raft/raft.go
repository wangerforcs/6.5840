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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

	snapShot []byte
	// Raft内记录的都是绝对索引，但Log需要用相对索引来访问
	LastIncludesIndex int
	LastIncludesTerm  int
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
	// e.Encode(rf.xxx)
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.LastIncludesIndex)
	e.Encode(rf.LastIncludesTerm)
	data := w.Bytes()
	rf.persister.Save(data, rf.snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var LastIncludesIndex int
	var LastIncludesTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&LastIncludesIndex) != nil ||
		d.Decode(&LastIncludesTerm) != nil {
		DPrintf("Server %d read persist error\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.LastIncludesIndex = LastIncludesIndex
		rf.LastIncludesTerm = LastIncludesTerm
		rf.lastApplied = LastIncludesIndex
		rf.commitIndex = LastIncludesIndex
	}
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

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	rf.snapShot = data
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
	idx = rf.getRelativeIndex(idx)
	if idx < 0 {
		return -1
	}
	return rf.log[idx].Term
}

func (rf *Raft) getRelativeIndex(index int) int {
	return index - rf.LastIncludesIndex
}

func (rf *Raft) getAbsoluteIndex(index int) int {
	return index + rf.LastIncludesIndex
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		return
	}
	// 很重要的一步，可以在一定程度上避免多个节点同时选举
	// 论文中all servers 2要求的是If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	// 即RPC request或者response时发现高任期需要将自己变为follower，无论是vote还是appendentry
	// 但这样的话，如果有三台服务器0,1,2，此时0是leader，突然从0-1断开了，于是1
	// election超时增加任期，那么重连后给0发送requestvote时，0会变成follower，这样看似是错误的
	// 实际上是不会影响的，因为如果0有新提交的话，真正可行的leader(此时是follower)不会投票给错误的候选者1
	if args.Term > rf.currentTerm {
		DPrintf("Server %d update term to %d in RequestVote\n", rf.me, args.Term)
		rf.becomeFollower(args.Term)
		sendCh(rf.voteCh)
	}
	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getAbsoluteIndex(rf.getLastLogIndex())) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			sendCh(rf.voteCh)
			DPrintf("%d server send vote to %d server %d term in RequestVote\n", rf.me, args.CandidateId, args.Term)
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
	Xterm  int // 与领导者冲突的任期号
	Xindex int // 与领导者冲突的日志索引
	// 多余了，Xterm可以完成相同的功能
	Xlen int // 空白的日志长度
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("%d server %d term receive heartbeat from %d server %d term\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	DPrintf("server %d snapshot state: %d %d server state : log index %d commit index %d\n", rf.me, rf.LastIncludesIndex, rf.LastIncludesTerm, rf.getAbsoluteIndex(len(rf.log)-1), rf.commitIndex)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		// partitioned leader
		// 旧Leader始终在一个小的分区中运行，而较大的分区会进行新的选举，最终成功选出一个新的Leader
		// fmt.Printf("why leader is behind\n")
		return
	}
	if args.Term > rf.currentTerm {
		DPrintf("Server %d update term to %d in Append\n", rf.me, args.Term)
		rf.becomeFollower(args.Term)
	}
	// reset election timer even log does not match
	// args.LeaderId is the current term's LeaderId
	sendCh(rf.appendCh)
	reply.Term = rf.currentTerm
	absolute := rf.getAbsoluteIndex(len(rf.log))
	if args.PrevLogIndex >= absolute {
		reply.Success = false

		reply.Xindex = absolute
		reply.Xterm = -1
		return
	}
	relative := rf.getRelativeIndex(args.PrevLogIndex)
	// 会收到在leader log但不在follower log的日志吗？不会
	if relative < 0 {
		// 按理说不会出现这样，即leader中记录的下一条服务器日志比服务器snapshot的还小
		DPrintf("fuck\n")
	}
	if relative >= 0 && rf.log[relative].Term != args.PrevLogTerm {
		reply.Success = false

		conflictid := args.PrevLogIndex
		reply.Xterm = rf.log[relative].Term
		for rf.log[rf.getRelativeIndex(conflictid)-1].Term == reply.Xterm {
			conflictid--
		}
		reply.Xindex = conflictid
		return
	}
	reply.Success = true
	rf.log = append(rf.log[:rf.getRelativeIndex(args.PrevLogIndex+1)], args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitTo(Min(args.LeaderCommit, rf.getAbsoluteIndex(len(rf.log)-1)))
	}

	DPrintf("%d becomes follwer in AppendEntries\n", rf.me)
	rf.becomeFollower(args.Term)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludesIndex int
	LastIncludesTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// md 这个地方笔误写成了defer rf.mu.lock()，导致一直出问题有个服务器一直找不到消息，太智障了，关键日志已经能发现在这肯定出现死锁了，愣是没看出来
	// 因为这个中间隔了快一周不想写，后来继续推进时候突然就能发现了 lab3最激动的一集
	/*
	Test (3D): snapshots basic ...
	... Passed --   7.0  3  138   47870  229
	Test (3D): install snapshots (disconnect) ...
	... Passed --  55.4  3 1238  488861  299
	Test (3D): install snapshots (disconnect+unreliable) ...
	... Passed --  74.2  3 1698  601718  316
	Test (3D): install snapshots (crash) ...
	... Passed --  35.9  3  740  391910  349
	Test (3D): install snapshots (unreliable+crash) ...
	... Passed --  42.0  3  868  406063  315
	Test (3D): crash and restart all servers ...
	... Passed --  14.9  3  312   85408   67
	Test (3D): snapshot initialization after crash ...
	... Passed --   3.3  3   76   19782   14
	PASS
	ok      6.5840/raft     232.646s
	*/
	defer rf.mu.Unlock()
	DPrintf("Server %d(until %d) receive snapshot(until %d) from %d server\n", rf.me, rf.LastIncludesIndex, args.LastIncludesIndex, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	sendCh(rf.appendCh)
	reply.Term = rf.currentTerm
	relative := rf.getRelativeIndex(args.LastIncludesIndex)
	if args.LastIncludesIndex <= rf.LastIncludesIndex {
		DPrintf("fuck2\n")
		return
	}
	DPrintf("Server %d install snapshot from %d\n", rf.me, args.LeaderId)
	// go func (args* InstallSnapshotArgs){
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if relative < len(rf.log)-1 {
		rf.log = rf.log[relative:]
	} else {
		rf.log = make([]LogEntry, 1)
		// 很重要 basic就在这卡了点，原本是都把第0项计为-1
		rf.log[0].Term = args.LastIncludesTerm
	}
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludesTerm,
		SnapshotIndex: args.LastIncludesIndex,
	}
	go func(rf *Raft, msg ApplyMsg) {
		rf.applyCh <- msg
	}(rf, msg)
	rf.snapShot = args.Data
	rf.LastIncludesIndex = args.LastIncludesIndex
	rf.LastIncludesTerm = args.LastIncludesTerm
	if rf.commitIndex < args.LastIncludesIndex {
		rf.commitIndex = args.LastIncludesIndex
	}
	if rf.lastApplied < args.LastIncludesIndex {
		rf.lastApplied = args.LastIncludesIndex
	}
	rf.persist()
	// }(args)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.commitIndex < index || index <= rf.LastIncludesIndex {
		return
	}
	DPrintf("Server %d is taking snapshot until index %d\n", rf.me, index)
	rf.snapShot = snapshot
	// 这句要在下一句前面，不然会导致LastIncludesIndex被修改，debug发现的，更改后报错从无限循环或apply不同 变为failed to reach agreement
	relative := rf.getRelativeIndex(index)
	rf.LastIncludesIndex = index
	rf.LastIncludesTerm = rf.log[relative].Term
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	// 初始时有一个空的log，后续有一个旧的log
	rf.log = rf.log[relative:]
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
		index = rf.getAbsoluteIndex(len(rf.log))
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.persist()
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
		DPrintf("%d server %v time\n", rf.me, electionTime)
		switch state {
		case Follower:
			select {
			case <-rf.voteCh:
			case <-rf.appendCh:
			case <-time.After(electionTime):
				DPrintf("Server %d is becoming candidate\n", rf.me)
				rf.newElection()
			}
		case Candidate:
			select {
			case <-rf.voteCh:
			case <-rf.appendCh:
			case <-time.After(electionTime):
				DPrintf("Candidate %d starts new selection\n", rf.me)
				rf.newElection()
			}
		case Leader:
			DPrintf("Leader %d is sending heartbeats\n", rf.me)
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
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.LastIncludesIndex = 0
	rf.LastIncludesTerm = -1

	rf.applyCh = applyCh
	rf.voteCh = make(chan bool, 1)
	rf.appendCh = make(chan bool, 1)

	// 初始空的占位log，方便边界处理
	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getAbsoluteIndex(len(rf.log))
	}

	LogInit()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) newElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()
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
				LastLogIndex: rf.getAbsoluteIndex(rf.getLastLogIndex()),
				LastLogTerm:  rf.getLastLogTerm(),
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(idx, &args, &reply)
			DPrintf("Server %d request vote from %d server\n", rf.me, idx)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					sendCh(rf.voteCh)
					return
				}
				if reply.VoteGranted {
					votes++
				}
				if votes > len(rf.peers)/2 && rf.state == Candidate {
					rf.becomeLeader()
					sendCh(rf.voteCh)
					DPrintf("%d becomes leader\n", rf.me)
				}
			}
		}(i)
	}
}

func (rf *Raft) newAppendLog() {
	DPrintf("server %d snapshot state: %d %d server state : log index %d commit index %d\n", rf.me, rf.LastIncludesIndex, rf.LastIncludesTerm, rf.getAbsoluteIndex(len(rf.log)-1), rf.commitIndex)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.getPrevLogIndex(i),
			LeaderCommit: rf.commitIndex,
		}
		sendingSnap := false
		if args.PrevLogIndex < rf.LastIncludesIndex {
			sendingSnap = true
		} else if args.PrevLogIndex < rf.getAbsoluteIndex(rf.getLastLogIndex()) {
			args.Entries = rf.log[rf.getRelativeIndex(args.PrevLogIndex+1):]
		} else {
			args.Entries = make([]LogEntry, 0)
		}
		if sendingSnap {
			go rf.solveSnapshot(i)
		} else {
			// 发送日志或心跳
			args.PrevLogTerm = rf.getPrevLogTerm(i)
			go rf.solveAppendEntries(i, &args)
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) solveSnapshot(idx int) {
	DPrintf("Leader %d is sending snapshot to %d server(nextid %d) until index %d\n", rf.me, idx, rf.getPrevLogIndex(idx)+1, rf.LastIncludesIndex)
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludesIndex: rf.LastIncludesIndex,
		LastIncludesTerm:  rf.LastIncludesTerm,
		Offset:            0,
		Data:              rf.snapShot,
		Done:              true,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(idx, &args, &reply)
	DPrintf("receive reply of snapshot")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("in reply lock\n")
	if !ok {
		DPrintf("not ok\n")
	}
	if ok {
		DPrintf("ok")
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		if rf.state != Leader {
			return
		}
		DPrintf("Server %d successfully send snapshot to %d server update nextid to %d\n", rf.me, idx, rf.LastIncludesIndex+1)
		rf.nextIndex[idx] = rf.LastIncludesIndex + 1
		rf.matchIndex[idx] = rf.LastIncludesIndex
	}
}

func (rf *Raft) solveAppendEntries(idx int, args *AppendEntriesArgs) {
	DPrintf("Server %d is sending entries to %d server nextid %d\n", rf.me, idx, rf.getPrevLogIndex(idx))
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(idx, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		if rf.state != Leader {
			return
		}
		if reply.Success {
			rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[idx] = rf.matchIndex[idx] + 1
			for N := rf.getAbsoluteIndex((len(rf.log) - 1)); N > rf.commitIndex; N-- {
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
			// 这里的处理还存疑
			DPrintf("fuck xindex return %d\n", reply.Xindex)
			rf.nextIndex[idx] = reply.Xindex
			if rf.nextIndex[idx] <= rf.LastIncludesIndex {
				// go rf.solveSnapshot(idx)
				return
			}
			if reply.Xterm != -1 {
				for i := rf.getRelativeIndex(rf.getPrevLogIndex(idx)); i >= 1; i-- {
					if rf.log[i-1].Term == reply.Xterm {
						rf.nextIndex[idx] = rf.getAbsoluteIndex(i)
						break
					}
				}
			}
		}
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getAbsoluteIndex(len(rf.log))
		// rf.matchIndex[i] =
	}
}

func (rf *Raft) commitTo(commitIndex int) {
	rf.commitIndex = commitIndex
	if rf.commitIndex > rf.lastApplied {
		// entries := append([]LogEntry{}, rf.log[(rf.lastApplied+1):(rf.commitIndex+1)]...)
		relativeApply := rf.getRelativeIndex(rf.lastApplied)
		relativeLast := rf.getRelativeIndex(rf.commitIndex)
		entries := append([]LogEntry{}, rf.log[(relativeApply+1):(relativeLast+1)]...)
		go func(last int, Entries []LogEntry) {
			for idx, entry := range Entries {
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: last + idx}
				rf.mu.Lock()
				rf.lastApplied = last + idx
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, entries)
	}
}
