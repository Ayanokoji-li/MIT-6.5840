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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/logger"
	tester "6.5840/tester1"
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

const (
	TIMEOUT_LIMIT = time.Duration(5) * time.Millisecond
	TICKER_INTER  = time.Duration(10) * time.Millisecond
	RETYR_WAIT    = time.Duration(90) * time.Millisecond
)

const (
	FOLLOWER  int = 0
	CANDIDATE int = 1
	LEADER    int = 2
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state int
	timer *time.Timer

	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int // candidateId that received vote in current term (or null if none)

	log          []LogEntry
	commitIndex  int   // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied  int   // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex    []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex   []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	apply_chan   chan ApplyMsg
	apply_signal chan bool

	start_mu sync.Mutex
}

func (rf *Raft) reset_timer() {
	var ms int64 = 75
	if rf.state != LEADER {
		ms += rand.Int63()%128 + 75
	}
	logger.Log(logger.DTimer, "S%d: Timer reseted for %d ms", rf.me, ms)
	rf.timer = time.NewTimer(time.Millisecond * time.Duration(ms))
}

type RaftUpdateOption func(*Raft)
type RaftGetIntOption func(*Raft) int
type RaftGetArrOption func(*Raft) []int

func (rf *Raft) update(options ...RaftUpdateOption) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for _, option := range options {
		option(rf)
	}
}

func (rf *Raft) getInt(options ...RaftGetIntOption) []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var ret []int
	for _, option := range options {
		ret = append(ret, option(rf))
	}

	return ret
}

func (rf *Raft) getArr(options ...RaftGetArrOption) [][]int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var ret [][]int
	for _, option := range options {
		ret = append(ret, option(rf))
	}

	return ret
}

func (rf *Raft) getLog() []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.log
}

func rfUpdateState(state int) RaftUpdateOption {
	return func(rf *Raft) {
		rf.state = state
	}
}

func rfUpdateTimer() RaftUpdateOption {
	return func(rf *Raft) {
		rf.reset_timer()
	}
}

func rfUpdateCurTerm(term int) RaftUpdateOption {
	return func(rf *Raft) {
		rf.currentTerm = term
	}
}

func rfUpdateVotedFor(voted int) RaftUpdateOption {
	return func(rf *Raft) {
		rf.votedFor = voted
	}
}

func rfUpdateAllNextID() RaftUpdateOption {
	return func(rf *Raft) {
		for index := range rf.nextIndex {
			rf.nextIndex[index] = len(rf.log)
		}
	}
}

func rfUpdateNextID(server int, nextID int) RaftUpdateOption {
	return func(rf *Raft) {
		nextID = max(0, nextID)
		rf.nextIndex[server] = nextID
	}
}

func rfUpdateOneLog(log LogEntry) RaftUpdateOption {
	return func(rf *Raft) {
		logger.Log(logger.DLog, "S%d: update log %+v", rf.me, log)
		rf.log = append(rf.log, log)
	}
}

func rfUpdateMulLog(logs []LogEntry, prev_index int) RaftUpdateOption {
	return func(rf *Raft) {
		logger.Log(logger.DLog, "S%d: append log %+v from index %d", rf.me, logs, prev_index)
		rf.log = append(rf.log[:prev_index+1], logs...)
	}
}

func rfUpdataCommID(commID int) RaftUpdateOption {
	return func(rf *Raft) {
		if commID > rf.commitIndex {
			rf.commitIndex = min(commID, len(rf.log))
			rf.apply_signal <- true
			logger.Log(logger.DCommit, "S%d: update commitID %d, last apply %d", rf.me, commID, rf.lastApplied)
		}
	}
}

func rfUpdateLastApplied(last_applied int) RaftUpdateOption {
	return func(rf *Raft) {
		rf.lastApplied = last_applied
	}
}

func rfUpdateMatchID() RaftUpdateOption {
	return func(rf *Raft) {
		for i := range rf.matchIndex {
			rf.matchIndex[i] = -1
		}
	}
}

func rfGetState() RaftGetIntOption {
	return func(rf *Raft) int {
		return rf.state
	}
}

func rfGetCurTerm() RaftGetIntOption {
	return func(rf *Raft) int {
		return rf.currentTerm
	}
}

func rfGetVoteFor() RaftGetIntOption {
	return func(rf *Raft) int {
		return rf.votedFor
	}
}

func rfGetMe() RaftGetIntOption {
	return func(rf *Raft) int {
		return rf.me
	}
}

func rfGetCommID() RaftGetIntOption {
	return func(rf *Raft) int {
		return rf.commitIndex
	}
}

func rfGetLastApplied() RaftGetIntOption {
	return func(rf *Raft) int {
		return rf.lastApplied
	}
}

func rfGetLogLen() RaftGetIntOption {
	return func(rf *Raft) int {
		return len(rf.log)
	}
}

func rfGetNextID() RaftGetArrOption {
	return func(rf *Raft) []int {
		return rf.nextIndex
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) GetDetailState() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state
}

func (rf *Raft) is_timeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	select {
	case <-rf.timer.C:
		{
			return true
		}
	default:
		{
			return false
		}
	}
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
	Term        int // candidate’s term
	CandidateID int // candidate requesting vote

	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	to_vote := args.CandidateID
	require_term := args.Term
	last_logID := args.LastLogIndex
	last_term := args.LastLogTerm

	logs := rf.getLog()
	rf_metadata := rf.getInt(rfGetCurTerm(), rfGetMe(), rfGetVoteFor())
	cur_term := rf_metadata[0]
	me := rf_metadata[1]
	vote_for := rf_metadata[2]
	me_last_logID := len(logs) - 1

	me_last_term := 0
	if me_last_logID != -1 {
		me_last_term = logs[me_last_logID].Term
	}

	reply.Term = cur_term
	if last_term >= me_last_term && last_logID >= me_last_logID {
		if require_term > cur_term {
			rf.update(rfUpdateCurTerm(require_term), rfUpdateState(FOLLOWER), rfUpdateVotedFor(to_vote), rfUpdateTimer())

			reply.VoteGranted = true

			logger.Log(logger.DVote, "S%d <- S%d: Got vote", to_vote, me)
		} else if vote_for == -1 || vote_for == to_vote {
			rf.update(rfUpdateState(FOLLOWER), rfUpdateTimer())

			reply.VoteGranted = true

			logger.Log(logger.DVote, "S%d <- S%d: Got vote", to_vote, me)
		} else {

			if require_term < cur_term {
				logger.Log(logger.DInfo, "S%d </ S%d: Don't vote, out of date. cur term: %d, request term: %d", to_vote, me, cur_term, require_term)
			} else if vote_for != to_vote {
				logger.Log(logger.DInfo, "S%d </ S%d: Don't vote, have voted to S%d", to_vote, me, vote_for)
			}

			reply.VoteGranted = false
		}
	} else {
		logger.Log(logger.DInfo, "S%d </ S%d: Don't vote, last term: %d : %d last log comm: %d : %d", to_vote, me, last_term, me_last_term, last_logID, me_last_logID)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	logger.Log(logger.DVote, "S%d -> S%d: Request vote", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) tryLeader() bool {
	logs := rf.getLog()
	rf_metadata := rf.getInt(rfGetMe(), rfGetCurTerm(), rfGetState())
	me := rf_metadata[0]
	cur_term := rf_metadata[1]
	state := rf_metadata[2]
	if state == FOLLOWER {
		cur_term += 1
	}
	last_term := 0
	last_logID := len(logs) - 1
	if last_logID >= 0 {
		last_term = logs[last_logID].Term
	}
	args := RequestVoteArgs{Term: cur_term, CandidateID: me, LastLogIndex: last_logID, LastLogTerm: last_term}

	rf.update(rfUpdateCurTerm(cur_term), rfUpdateState(CANDIDATE), rfUpdateVotedFor(me))
	connected_num := 1
	voteGranted_num := 1
	reply_chan := make(chan int, len(rf.peers))

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.requestVoteFromPeer(server, args, reply_chan, cur_term)
	}

	timer := time.NewTimer(TIMEOUT_LIMIT)
	defer timer.Stop()

	return rf.collectVotes(reply_chan, timer, connected_num, voteGranted_num, me)
}

func (rf *Raft) requestVoteFromPeer(server int, args RequestVoteArgs, reply_chan chan int, cur_term int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if ok {
		if reply.VoteGranted {
			reply_chan <- 1
		} else if reply.Term > cur_term {
			rf.update(rfUpdateCurTerm(reply.Term), rfUpdateState(FOLLOWER))
			reply_chan <- -1
		} else {
			reply_chan <- 0
		}
	}
}

func (rf *Raft) collectVotes(reply_chan chan int, timer *time.Timer, connected_num, voteGranted_num, me int) bool {
	for {
		select {
		case reply := <-reply_chan:
			if reply == -1 {
				logger.Log(logger.DFollower, "S%d: Have been follower, stop requesting vote", me, voteGranted_num, connected_num)
				return false
			}
			connected_num++
			if reply == 1 {
				voteGranted_num++
			}
			if connected_num == len(rf.peers) {
				return rf.checkVotes(voteGranted_num, connected_num, me)
			}
		case <-timer.C:
			return rf.checkVotes(voteGranted_num, connected_num, me)
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.update(rfUpdateState(LEADER), rfUpdateAllNextID(), rfUpdateMatchID())
	logger.Log(logger.DLeader, "S%d: Become leader", rf.me)
}

func (rf *Raft) checkVotes(voteGranted_num, connected_num, me int) bool {
	logger.Log(logger.DVote, "S%d: Recv vote %d in %d", me, voteGranted_num, connected_num)
	_, cur_state := rf.GetDetailState()

	if connected_num > 1 && voteGranted_num >= (connected_num+2)/2 && cur_state == CANDIDATE {
		rf.becomeLeader()
		return true
	}

	return false
}

type AppendEntriesArgs struct {
	Term         int // leader’s term
	LeaderID     int // leader's ID
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	leader_term := args.Term
	leader_id := args.LeaderID
	logs := args.Entries
	commID := args.LeaderCommit
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm

	rf_metadata := rf.getInt(rfGetCurTerm(), rfGetMe())
	cur_term := rf_metadata[0]
	me := rf_metadata[1]
	me_logs := rf.getLog()

	if leader_term < cur_term {
		reply.Term = cur_term
		reply.Success = false
		logger.Log(logger.DAppend, "S%d <- S%d: Leader out of date", leader_id, me)
	} else {
		rf.update(rfUpdateVotedFor(leader_id), rfUpdateTimer(), rfUpdateCurTerm(leader_term), rfUpdateState(FOLLOWER))

		reply.Term = leader_term
		if prevLogIndex != -1 {
			reply.Success = false
			if prevLogIndex >= len(me_logs) {
				logger.Log(logger.DAppend, "S%d <- S%d: Fail, lack prev logs", leader_id, me)
				return
			}

			if prevLogTerm != me_logs[prevLogIndex].Term {
				logger.Log(logger.DAppend, "S%d <- S%d: Fail, wrong prev log term", leader_id, me)
			}
		}

		rf.update(rfUpdateMulLog(logs, prevLogIndex), rfUpdataCommID(commID))

		reply.Success = true
		logger.Log(logger.DAppend, "S%d <- S%d: Update status successfully", leader_id, me)
	}
}

func (rf *Raft) sendAppend(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	logger.Log(logger.DAppend, "S%d -> S%d: log len: %d, prev id: %d, prev term: %d", rf.me, server, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendToPeer(server int, args AppendEntriesArgs, nextID int, log []LogEntry, reply_chan chan bool, me int, cur_term int, is_once bool) {
	// nextID = max(nextID, 0)
	args.PrevLogIndex = nextID - 1
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = log[args.PrevLogIndex].Term
	} else {
		args.PrevLogTerm = 0
	}

	if len(log) > nextID {
		args.Entries = log[nextID:]
	}
	state_chan := make(chan bool, 1)
	in_loop := true
	for in_loop {
		_, is_leader := rf.GetState()
		if !is_leader {
			logger.Log(logger.DAppend, "S%d: Not leader, stop Append to S%d", me, server)
			return
		}

		go func() {
			reply := AppendEntriesReply{}
			ok := rf.sendAppend(server, &args, &reply)
			if ok {
				state_chan <- reply.Success
				if !reply.Success {
					if reply.Term > cur_term {
						logger.Log(logger.DFollower, "S%d: out-of-date, become follower. Self: %d, reply: %d", me, cur_term, reply.Term)
						rf.update(rfUpdateCurTerm(reply.Term), rfUpdateState(FOLLOWER))
					} else {
						rf.update(rfUpdateNextID(server, nextID-1))
						logger.Log(logger.DAppend, "S%d: fail to update S%d, try resend from %d", me, server, nextID-1)
					}
				} else {
					rf.update(rfUpdateNextID(server, len(log)))
					logger.Log(logger.DAppend, "S%d: Update S%d next id: %d", me, server, len(log))
				}
			} else {
				state_chan <- false
			}
		}()

		select {
		case to_reply := <-state_chan:
			{
				in_loop = !to_reply
				reply_chan <- to_reply
			}
		case <-time.After(TIMEOUT_LIMIT):
			{
				in_loop = !is_once
				logger.Log(logger.DWarn, "S%d: Send append entries time-out, wait", me)
				time.Sleep(RETYR_WAIT)
			}
		}
	}
}

func (rf *Raft) sendAppends(is_once bool) {

	rf_int_metadata := rf.getInt(rfGetCurTerm(), rfGetMe(), rfGetCommID())
	rf_arr_metadata := rf.getArr(rfGetNextID())
	log := rf.getLog()
	cur_term := rf_int_metadata[0]
	me := rf_int_metadata[1]
	commID := rf_int_metadata[2]
	nextIDs := rf_arr_metadata[0]
	args := AppendEntriesArgs{Term: cur_term, LeaderID: me, Entries: []LogEntry{}, LeaderCommit: commID}
	reply_chan := make(chan bool, len(rf.peers))

	for server := range rf.peers {
		if server == me {
			continue
		}
		go rf.sendAppendToPeer(server, args, nextIDs[server], log, reply_chan, me, cur_term, is_once)
	}
	if len(log) > commID+1 {

		go rf.appendCollector(reply_chan, log)
	}
}

func (rf *Raft) appendCollector(reply_chan chan bool, logs []LogEntry) {
	reply_num := 0
	for reply := range reply_chan {
		_, is_leader := rf.GetState()
		if !is_leader {
			return
		}

		if reply {
			reply_num += 1
			if reply_num >= len(rf.peers)/2 {
				rf.commitLog(logs)
				return
			}
		}
	}
}

func (rf *Raft) commitLog(logs []LogEntry) {
	rf.update(rfUpdataCommID(len(logs) - 1))
	logger.Log(logger.DCommit, "S%d: log has replicated more than half, commit log %v", rf.me, logs[len(logs)-1])
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
	rf.start_mu.Lock()
	defer rf.start_mu.Unlock()
	rf_metadata := rf.getInt(rfGetLogLen(), rfGetCurTerm(), rfGetState())
	commID := rf_metadata[0]
	cur_term := rf_metadata[1]
	isLeader := rf_metadata[2] == LEADER

	if isLeader {
		logger.Log(logger.DServe, "S%d: get start service call", rf.me)
		rf.update(rfUpdateOneLog(LogEntry{Command: command, Term: cur_term}))
		go rf.sendAppends(false)
		logger.Log(logger.DInfo, "S%d: cmd %v start return commID %d term %d is leader %v", rf.me, command, commID+1, cur_term, isLeader)
	}
	return commID + 1, cur_term, isLeader
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
	rf_metadata := rf.getInt(rfGetMe())
	me := rf_metadata[0]
	for !rf.killed() {

		if rf.is_timeout() {

			logger.Log(logger.DTimer, "S%d: Timer time-out", me)

			_, is_leader := rf.GetState()
			if !is_leader {
				logger.Log(logger.DInfo, "S%d: haven't recived append entries, become candidator", me)
				is_leader = rf.tryLeader()
			}

			if is_leader {
				logger.Log(logger.DLeader, "S%d: Start send append entries", me)
				rf.sendAppends(true)
			}

			rf.update(rfUpdateTimer())
		} else {
			time.Sleep(TICKER_INTER)
		}
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
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{timer: time.NewTimer(time.Duration((me+1)*10) * time.Millisecond)}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.apply_chan = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.apply_signal = make(chan bool, 100)

	rf.log = []LogEntry{}
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go func() {
		me := rf.me
		for range rf.apply_signal {
			rf_metadata := rf.getInt(rfGetCommID(), rfGetLastApplied())
			commID := rf_metadata[0]
			last_apply := rf_metadata[1]
			logs := rf.getLog()

			i := last_apply + 1
			for ; i <= commID && i < len(logs); i++ {

				apply_msg := ApplyMsg{CommandValid: true, Command: logs[i].Command, CommandIndex: i + 1}
				logger.Log(logger.DCommit, "S%d: to apply msg %v", me, apply_msg)

				rf.apply_chan <- apply_msg
			}

			rf.update(rfUpdateLastApplied(i - 1))
		}
	}()

	return rf
}
