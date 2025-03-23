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

	"bytes"
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	LEADER_RESET_TIME        = 40
	FOLLOWER_RESET_BASE_TIEM = 60
	TIMEOUT_LIMIT            = time.Duration(15) * time.Millisecond
	TICKER_INTER             = time.Duration(5) * time.Millisecond
	MAX_LOG_LEN              = 30
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

	State int
	timer *time.Timer

	CurrentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int // candidateId that received vote in current term (or null if none)
	voteChan    chan bool

	Log         []LogEntry
	appendChan  chan bool
	CommitIndex int   // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied int   // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	apply_chan  chan ApplyMsg

	start_mu sync.Mutex
}

func (rf *Raft) reset_timer() {
	var ms int64 = LEADER_RESET_TIME
	if rf.State != LEADER {
		ms = rand.Int63()%128 + FOLLOWER_RESET_BASE_TIEM
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

	return rf.Log
}

func rfUpdateState(state int) RaftUpdateOption {
	return func(rf *Raft) {
		rf.State = state
	}
}

func rfUpdateTimer() RaftUpdateOption {
	return func(rf *Raft) {
		rf.reset_timer()
	}
}

func rfUpdateCurTerm(term int) RaftUpdateOption {
	return func(rf *Raft) {
		logger.Log(logger.DInfo, "S%d: update term from %d to %d", rf.me, rf.CurrentTerm, term)
		rf.CurrentTerm = max(rf.CurrentTerm, term)
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
			rf.nextIndex[index] = len(rf.Log)
		}
	}
}

func rfUpdateNextID(server int, nextID int) RaftUpdateOption {
	return func(rf *Raft) {
		nextID = max(0, nextID)
		rf.nextIndex[server] = nextID
	}
}

func rfUpdateLastApplied(last_applied int) RaftUpdateOption {
	return func(rf *Raft) {
		rf.LastApplied = max(rf.LastApplied, last_applied)
	}
}

func rfUpdateOneLog(log LogEntry) RaftUpdateOption {
	return func(rf *Raft) {
		rf.Log = append(rf.Log, log)
		rf.matchIndex[rf.me] = len(rf.Log)
		logger.Log(logger.DLog, "S%d: update log %+v total log len %d", rf.me, log, len(rf.Log))
		// rf.persist()
	}
}

func rfUpdateMulLog(logs []LogEntry, prev_index int) RaftUpdateOption {
	return func(rf *Raft) {
		rf.Log = append(rf.Log[:prev_index+1], logs...)
		logger.Log(logger.DLog, "S%d: append log %+v from index %d, log len %d", rf.me, logs, prev_index, len(rf.Log))
		// rf.persist()
	}
}

func rfUpdataCommID(commID int) RaftUpdateOption {
	return func(rf *Raft) {
		if commID <= rf.CommitIndex {
			return
		}
		rf.CommitIndex = min(commID, len(rf.Log)-1)
		logger.Log(logger.DCommit, "S%d: update cur commID %d", rf.me, rf.CommitIndex)
		rf.commitCheck(rf.CommitIndex, rf.LastApplied, rf.Log[:])
	}
}

func rfUpdateAllMatchID() RaftUpdateOption {
	return func(rf *Raft) {
		for i := range rf.matchIndex {
			rf.matchIndex[i] = -2
			if i == rf.me {
				rf.matchIndex[i] = len(rf.Log)
			}
		}
	}
}

func rfUpdateMatchID(server int, matchID int) RaftUpdateOption {
	return func(rf *Raft) {
		rf.matchIndex[server] = matchID
	}
}

func rfGetState() RaftGetIntOption {
	return func(rf *Raft) int {
		return rf.State
	}
}

func rfGetCurTerm() RaftGetIntOption {
	return func(rf *Raft) int {
		return rf.CurrentTerm
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
		return rf.CommitIndex
	}
}

func rfGetLogLen() RaftGetIntOption {
	return func(rf *Raft) int {
		return len(rf.Log)
	}
}

func rfGetLastApplied() RaftGetIntOption {
	return func(rf *Raft) int {
		return rf.LastApplied
	}
}

func rfGetNextID() RaftGetArrOption {
	return func(rf *Raft) []int {
		return rf.nextIndex
	}
}

func rfGetMatchID() RaftGetArrOption {
	return func(rf *Raft) []int {
		return rf.matchIndex
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.State == LEADER
}

func (rf *Raft) GetDetailState() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.CurrentTerm, rf.State
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

func (rf *Raft) commitCheck(commID int, last_applied int, log []LogEntry) {
	go func() {
		if commID != last_applied && commID >= 0 {
			last_applied += 1
			for ; last_applied <= commID; last_applied++ {
				logger.Log(logger.DCommit, "S%d: commit %d log %v", rf.me, last_applied, log[last_applied])
				rf.apply_chan <- ApplyMsg{CommandValid: true, Command: log[last_applied].Command, CommandIndex: last_applied + 1}
			}
			rf.update(rfUpdateLastApplied(last_applied - 1))
			rf.persist()
		}
	}()
}

func (rf *Raft) matchCheck(commID int, matchIndex []int, curTerm int, log []LogEntry) {
	go func() {
		new_commID := commID
		for {
			replic_num := 0
			for i, matchID := range matchIndex {
				logger.Log(logger.DCommit, "S%d: S%d's match is %d, cur commid %d", rf.me, i, matchID, commID)

				if matchID > new_commID {
					replic_num += 1
				}
			}
			if replic_num >= (len(rf.peers)+1)/2 {
				new_commID = new_commID + 1
				logger.Log(logger.DCommit, "S%d: majority with %d servers, update commID %d", rf.me, replic_num, new_commID)
				if new_commID == len(log)-1 {
					break
				}
			} else {
				logger.Log(logger.DCommit, "S%d: stop match check", rf.me)
				break
			}
		}
		if new_commID >= 0 && log[new_commID].Term == curTerm {
			rfUpdataCommID(new_commID)(rf)
		}

	}()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	logger.Log(logger.DPersist, "S%d: persist log with log len %d, commID %d, term %d, state %+v", rf.me, len(rf.Log), rf.CommitIndex, rf.CurrentTerm, rf.State)
	e.Encode(rf.State)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.CurrentTerm)
	if rf.CommitIndex < 0 {
		e.Encode(rf.Log)
	} else {
		e.Encode(rf.Log[:rf.CommitIndex+1])
	}
	// e.Encode(rf.Log)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		logger.Log(logger.DPersist, "S%d: no persist before", rf.me)
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var State int
	var CommitIndex int
	var CurrentTerm int
	var Log []LogEntry
	if d.Decode(&State) != nil || d.Decode(&CommitIndex) != nil || d.Decode(&CurrentTerm) != nil || d.Decode(&Log) != nil {
		logger.Log(logger.DPersist, "S%d: error persist before", rf.me)
	} else {
		rf.State = State
		rf.CommitIndex = CommitIndex
		rf.CurrentTerm = CurrentTerm
		rf.Log = Log

		if rf.State == LEADER {
			// rf.update(rfUpdateAllNextID(), rfUpdateAllMatchID())
			rf.becomeLeader()
		}

		logger.Log(logger.DPersist, "S%d: recovered term %d, commID %d, log %v, state %v", rf.me, rf.CurrentTerm, rf.CommitIndex, rf.Log, rf.State)
	}
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
	logger.Log(logger.DVote, "S%d: get vote request, cur logs %v", me, logs)

	me_last_term := 0
	if me_last_logID != -1 {
		me_last_term = logs[me_last_logID].Term
	}

	reply.Term = cur_term
	if last_term > me_last_term || (last_term == me_last_term && last_logID >= me_last_logID) {
		if require_term > cur_term {
			rf.update(rfUpdateState(FOLLOWER), rfUpdateVotedFor(to_vote), rfUpdateTimer())

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

func (rf *Raft) sendRequestVoteWithTimer(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	logger.Log(logger.DVote, "S%d -> S%d: Request vote", rf.me, server)

	res_chan := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		res_chan <- ok
	}()

	timer := time.NewTimer(TIMEOUT_LIMIT)

	select {
	case ok := <-res_chan:
		{
			return ok
		}
	case <-timer.C:
		{
			logger.Log(logger.DVote, "S%d -> S%d: Request time out", rf.me, server)
			return false
		}
	}
}

func (rf *Raft) setupVoteRoutines() {
	rf.voteChan = make(chan bool, 1)
	reply_chan := make(chan bool, len(rf.peers))
	me := rf.me
	args := RequestVoteArgs{CandidateID: me}

	go func() {
		for <-rf.voteChan {
			rf_metadata := rf.getInt(rfGetCurTerm(), rfGetState())
			logs := rf.getLog()
			cur_term := rf_metadata[0] + 1
			last_term := 0
			last_logID := len(logs) - 1
			if last_logID >= 0 {
				last_term = logs[last_logID].Term
			}
			rf.update(rfUpdateCurTerm(cur_term), rfUpdateState(CANDIDATE), rfUpdateVotedFor(me))

			args.Term = cur_term
			args.LastLogIndex = last_logID
			args.LastLogTerm = last_term

			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				peerID := i
				go func() {
					reply := RequestVoteReply{}
					ok := rf.sendRequestVoteWithTimer(peerID, &args, &reply)
					if ok {
						reply_chan <- reply.VoteGranted
						if !reply.VoteGranted && reply.Term > cur_term {
							rf.update(rfUpdateState(FOLLOWER), rfUpdateCurTerm(reply.Term))
						}
					} else {
						reply_chan <- false
					}
				}()
			}

			connected_num := 1
			voteGranted_num := 1
			for connected_num < len(rf.peers) {
				reply := <-reply_chan
				connected_num += 1
				if reply {
					voteGranted_num += 1
				}
			}
			rf.voteChan <- rf.checkVotes(voteGranted_num, connected_num, me)
		}
	}()
}

func (rf *Raft) checkVotes(voteGranted_num, connected_num, me int) bool {
	logger.Log(logger.DVote, "S%d: Recv vote %d in %d", me, voteGranted_num, connected_num)
	_, cur_state := rf.GetDetailState()

	if connected_num > 1 && voteGranted_num >= (connected_num+2)/2 && cur_state == CANDIDATE {
		rf.becomeLeader()
		return true
	}

	rf.update(rfUpdateTimer(), rfUpdateVotedFor(-1))
	return false
}

func (rf *Raft) becomeLeader() {
	rf.update(rfUpdateState(LEADER), rfUpdateAllNextID(), rfUpdateAllMatchID())
	logger.Log(logger.DLeader, "S%d: Become leader", rf.me)
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
	logger.Log(logger.DAppend, "S%d: get vote request, cur logs %v", me, me_logs)

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
				return
			}
		}

		rf.update(rfUpdateMulLog(logs, prevLogIndex), rfUpdataCommID(commID))

		reply.Success = true
		logger.Log(logger.DAppend, "S%d <- S%d: Update status successfully", leader_id, me)
	}
}

func (rf *Raft) sendAppendWithTimer(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	logger.Log(logger.DAppend, "S%d -> S%d: args %+v", rf.me, server, args)

	res_chan := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		res_chan <- ok
	}()

	timer := time.NewTimer(TIMEOUT_LIMIT)

	select {
	case ok := <-res_chan:
		{
			return ok
		}

	case <-timer.C:
		{
			logger.Log(logger.DAppend, "S%d -> S%d: Append time out", rf.me, server)
			return false
		}
	}

}

func (rf *Raft) setupAppendRoutine() {
	rf.appendChan = make(chan bool)
	reply_chan := make(chan bool, len(rf.peers)-1)
	me := rf.me

	go func() {
		for <-rf.appendChan {
			logs := rf.getLog()
			rf_int_metadata := rf.getInt(rfGetCurTerm(), rfGetCommID())
			cur_term := rf_int_metadata[0]
			commID := rf_int_metadata[1]
			rf_arr_metadata := rf.getArr(rfGetNextID(), rfGetMatchID())
			nextIDs := rf_arr_metadata[0]
			matchIDs := rf_arr_metadata[1]
			logger.Log(logger.DAppend, "S%d: match id %v", me, matchIDs)

			for i := range rf.peers {
				if i == me {
					continue
				}

				peerID := i
				go func() {

					lastID := nextIDs[peerID] - 1
					last_term := 0
					if lastID >= 0 {
						last_term = logs[lastID].Term
					}

					args := AppendEntriesArgs{
						LeaderID:     me,
						LeaderCommit: commID,
						Term:         cur_term,
						PrevLogIndex: lastID,
						PrevLogTerm:  last_term,
					}
					if matchIDs[peerID] > -2 {
						args.LeaderCommit = commID
						if len(logs) > nextIDs[peerID] {
							args.Entries = logs[nextIDs[peerID]:]

							if len(args.Entries) > MAX_LOG_LEN {
								args.Entries = args.Entries[:MAX_LOG_LEN]
							}

							logger.Log(logger.DAppend, "S%d: send to S%d from id %d", me, peerID, nextIDs[peerID])
						}
					} else {
						args.LeaderCommit = -1
					}
					reply := AppendEntriesReply{}

					ok := rf.sendAppendWithTimer(peerID, &args, &reply)
					if ok {
						if reply.Success {
							if matchIDs[peerID] <= -2 {
								rf.update(rfUpdateMatchID(peerID, lastID))
								logger.Log(logger.DAppend, "S%d: match S%d", me, peerID)
							} else {
								rf.update(rfUpdateMatchID(peerID, nextIDs[peerID]+len(args.Entries)-1), rfUpdateNextID(peerID, nextIDs[peerID]+len(args.Entries)))
								logger.Log(logger.DAppend, "S%d: update S%d next id to %d", me, peerID, nextIDs[peerID]+len(args.Entries))
							}
						} else if reply.Term <= cur_term {
							rf.update(rfUpdateNextID(peerID, lastID))
							logger.Log(logger.DAppend, "S%d: fail to update S%d, set next id %d", me, peerID, lastID)
						} else {
							rf.update(rfUpdateState(FOLLOWER), rfUpdateCurTerm(reply.Term))
						}
						reply_chan <- reply.Success
					} else {
						reply_chan <- false
					}
				}()
			}

			for range len(rf.peers) - 1 {
				<-reply_chan
			}
			rf.matchCheck(commID, matchIDs, cur_term, logs)
		}
	}()
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
				rf.voteChan <- true
				is_leader = <-rf.voteChan
			}

			if is_leader {
				logger.Log(logger.DLeader, "S%d: Start send append entries", me)
				rf.appendChan <- true
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
	rf := &Raft{timer: time.NewTimer(time.Duration(1) * time.Millisecond)}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.apply_chan = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.State = FOLLOWER
	rf.CurrentTerm = 0
	rf.votedFor = -1
	rf.CommitIndex = -1
	rf.LastApplied = -1

	rf.Log = []LogEntry{}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.setupVoteRoutines()
	rf.setupAppendRoutine()
	go rf.ticker()
	logger.Log(logger.DServe, "S%d: started", rf.me)

	return rf
}
