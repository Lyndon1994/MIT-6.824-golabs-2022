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
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	DefaultVotedFor = -1
)

type NodeState int

const (
	_ NodeState = iota
	StateLeader
	StateCandidate
	StateFollower
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond   // used to wakeup applier goroutine after committing new entries
	replicatorCond []*sync.Cond // used to signal replicator goroutine to batch replicating entries
	state          NodeState

	currentTerm int // latest term server has seen (initialized to 0 ib first boot, increases monotonically)
	votedFor    int // candidateId that received vote in current term (or null if none)
	// log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1).
	// the first entry is a dummy entry which contains LastSnapshotTerm, LastSnapshotIndex and nil Command
	logs []Entry

	commitIndex int // index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of the highest entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == StateLeader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm,for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
// Invoked by candidates to gather votes
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != DefaultVotedFor && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.votedFor = args.Term, DefaultVotedFor
	}
	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader {
		return index, term, false
	}
	index = rf.getLastLog().Index + 1
	term = rf.currentTerm
	rf.logs = append(rf.logs, Entry{Term: term, Command: command, Index: index})
	rf.persist()
	DPrintf("[StartCommand] Leader %d get command %v,index %d", rf.me, command, index)
	rf.BroadcastHeartbeat(false)
	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()

		case <-rf.electionTimer.C:
			// 没有 Leader，Followers 无法与 Leader 保持心跳（Heart Beat），
			// 节点启动后在一个选举定时器周期内未收到心跳和投票请求，则状态转为候选者 Candidate 状态，且 Term 自增，
			// 并向集群中所有节点发送投票请求并且重置选举定时器。
			rf.mu.Lock()
			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) isLogUpToDate(term int, index int) bool {
	lastIndex := rf.getLastLog().Index
	lastTerm := rf.getLastLog().Term
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) ChangeState(state NodeState) {
	rf.state = state
	if state == StateFollower {
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	}
	if state == StateLeader {
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.getLastLog().Index + 1
		}
		rf.matchIndex[rf.me] = rf.getLastLog().Index
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
	rf.persist()
}

// StartElection 当一个节点开始竞选：
//
//增加自己的 currentTerm
//转为 Candidate 状态，其目标是获取超过半数节点的选票，让自己成为 Leader
//先给自己投一票
//并行地向集群中其它节点发送 RequestVote RPC 索要选票，如果没有收到指定节点的响应，它会反复尝试，直到发生以下三种情况之一：
//获得超过半数的选票：成为 Leader，并向其它节点发送 AppendEntries 心跳；
//收到来自 Leader 的 RPC：转为 Follower；
//其它两种情况都没发生，没人能够获胜(electionTimeout 已过)：增加 currentTerm，开始新一轮选举；
func (rf *Raft) StartElection() {
	rf.currentTerm += 1
	rf.ChangeState(StateCandidate)
	rf.votedFor = rf.me
	request := rf.genRequestVoteRequest()
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, request)

	grantedVotes := 1
	//rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			response := new(RequestVoteReply)
			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, response, peer, request, rf.currentTerm)
				// 如果term和state没有变化，则继续归票
				if rf.currentTerm == request.Term && rf.state == StateCandidate {
					if response.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.ChangeState(StateLeader)
							rf.BroadcastHeartbeat(true)
						}
					} else if response.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.ChangeState(StateFollower)
						rf.currentTerm, rf.votedFor = response.Term, -1
						//rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) BroadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartbeat {
			// need sending at once to maintain leadership
			go rf.replicateOneRound(peer)
		} else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) getFirstLog() *Entry {
	return &rf.logs[0]
}

func (rf *Raft) getLastLog() *Entry {
	return &rf.logs[len(rf.logs)-1]
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastLog().Term,
		LastLogIndex: rf.getLastLog().Index,
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(request *AppendEntriesArgs, response *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, DefaultVotedFor
	}
	rf.ChangeState(StateFollower)

	//if request.PrevLogIndex > rf.getLastLog().Index {
	//	response.Term, response.Success = rf.currentTerm, false
	//	return
	//}
	if request.PrevLogIndex < rf.getFirstLog().Index {
		response.Term, response.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId, request.PrevLogIndex, rf.getFirstLog().Index)
		return
	}

	if !rf.matchLog(request.PrevLogTerm, request.PrevLogTerm) {
		response.Term, response.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if lastIndex < request.PrevLogIndex {
			//1.  Follower 的 log 不够新，prevLogIndex 已经超出 log 长度
			response.ConflictTerm, response.ConflictIndex = -1, lastIndex+1
		} else {
			// 2. Follower prevLogIndex 处存在 log
			// 向主节点上报信息，加速下次复制日志
			// 当PreLogTerm与当前日志的任期不匹配时，找出日志第一个不匹配任期的index
			firstIndex := rf.getFirstLog().Index
			response.ConflictTerm = rf.logs[request.PrevLogIndex-firstIndex].Term
			index := request.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictTerm {
				index--
			}
			response.ConflictIndex = index
		}
		return
	}

	rf.logs = append(rf.logs[:request.PrevLogIndex-rf.getFirstLog().Index+1], request.Entries...)
	if request.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(StateFollower, request.LeaderCommit)
	}
	response.Term, response.Success = rf.currentTerm, true
}

// TODO ?
func (rf *Raft) getLogTermWithIndex(globalIndex int) int {
	return rf.logs[globalIndex-rf.getFirstLog().Index].Term
}

func (rf *Raft) matchLog(prevLogTerm, PrevLogIndex int) bool {
	return rf.getLogTermWithIndex(PrevLogIndex) == prevLogTerm
}

// Leader 向 Follower 发送复制请求
func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		// only snapshot can catch up
		//request := rf.genInstallSnapshotRequest()
		//rf.mu.Unlock()
		//response := new(InstallSnapshotResponse)
		//if rf.sendInstallSnapshot(peer, request, response) {
		//	rf.mu.Lock()
		//	rf.handleInstallSnapshotResponse(peer, request, response)
		//	rf.mu.Unlock()
		//}
	} else {
		// just entries can catch up
		request := rf.genAppendEntriesRequest(prevLogIndex)
		rf.mu.Unlock()
		response := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
		} else {
			DPrintf("[sendAppendEntry Failed]")
		}
	}
}

func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesArgs {

	request := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.getLogTermWithIndex(prevLogIndex),
		LeaderCommit: rf.commitIndex,
	}
	DPrintf("[genAppendEntries]prevlogIdx=%v, loglen=%v, getlastlog.idx=%v", prevLogIndex, len(rf.logs), rf.getLastLog().Index)
	if prevLogIndex == rf.getLastLog().Index {
		//heartbeat
		request.Entries = []Entry{}
	} else {
		entries := make([]Entry, 0)
		entries = append(entries, rf.logs[prevLogIndex-rf.getFirstLog().Index+1:]...)
		request.Entries = entries
	}
	return request
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesArgs, response *AppendEntriesReply) {
	if rf.state != StateLeader {
		return
	}
	if response.Term > rf.currentTerm {
		rf.currentTerm = response.Term
		rf.ChangeState(StateFollower)
		return
	}
	if response.Success {
		rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.updateCommitIndex(StateLeader, 0)
	} else {
		// 普通做法
		//rf.nextIndex[peer] -= 1

		// 优化做法 TODO understanding
		rf.nextIndex[peer] = response.ConflictIndex
		if response.ConflictTerm != -1 {
			firstIndex := rf.getFirstLog().Index
			for i := request.PrevLogIndex; i >= firstIndex; i-- {
				if rf.logs[i-firstIndex].Term == response.ConflictTerm {
					rf.nextIndex[peer] = i + 1
					break
				}
			}
		}
	}
}

func (rf *Raft) updateCommitIndex(state NodeState, leaderCommit int) {
	if state != StateLeader {
		if leaderCommit > rf.commitIndex {
			lastNewIndex := rf.getLastLog().Index
			if leaderCommit >= lastNewIndex {
				rf.commitIndex = lastNewIndex
			} else {
				rf.commitIndex = leaderCommit
			}
		}
		rf.applyCond.Signal()
		return
	}
	if state == StateLeader {
		// TODO understanding
		rf.commitIndex = rf.getFirstLog().Index
		for index := rf.getLastLog().Index; index >= rf.getFirstLog().Index; index-- {
			sum := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= index {
					sum += 1
				}
			}
			if sum >= len(rf.peers)/2+1 && rf.getLogTermWithIndex(index) == rf.currentTerm {
				rf.commitIndex = index
				break
			}

		}
		rf.applyCond.Signal()
		return
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) appendNewEntry(command interface{}) *Entry {
	newLog := Entry{
		Index:   rf.getLastLog().Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, newLog)
	return &newLog
}

// 对于异步 apply，其触发方式无非两种，leader 提交了新的日志或者 follower 通过 leader 发来的 leaderCommit 来更新 commitIndex。
// 很多人实现的时候可能顺手就在这两处异步启一个协程把 [lastApplied + 1, commitIndex] 的 entry push 到 applyCh 中，
// 但其实这样子是可能重复发送 entry 的，原因是 push applyCh 的过程不能够持锁，那么这个 lastApplied 在没有 push 完之前就无法得到更新，从而可能被多次调用。
// 虽然只要上层服务可以保证不重复 apply 相同 index 的日志到状态机就不会有问题，但我个人认为这样的做法是不优雅的。
// 考虑到异步 apply 时最耗时的步骤是 apply channel 和 apply 日志到状态机，其他的都不怎么耗费时间。
// 因此我们完全可以只用一个 applier 协程，让其不断的把 [lastApplied + 1, commitIndex] 区间的日志 push 到 applyCh 中去。
// 这样既可保证每一条日志只会被 exactly once 地 push 到 applyCh 中，也可以使得日志 apply 到状态机和 raft 提交新日志可以真正的并行。
// 我认为这是一个较为优雅的异步 apply 实现。
// a dedicated applier goroutine to guarantee that each log will be push into applyCh exactly once, ensuring that service's applying entries and raft's committing entries can be parallel
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				//CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
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
	// Your initialization code here (2A, 2B, 2C).
	// 一个应用 Raft 协议的集群在刚启动（或 Leader 宕机）时，所有节点的状态都是 Follower，初始 Term（任期）为 0。同时启动选举定时器。
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       DefaultVotedFor,
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
	}

	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()

	return rf
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(150) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	// 每个节点的选举定时器超时时间都在 300~600 毫秒之间且并不一致（避免同时发起选举）。
	return time.Duration(300+rand.Intn(300)) * time.Millisecond
}
