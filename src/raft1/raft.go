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

	log         []LogEntry
	commitIndex int   // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int   // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

}

func (rf *Raft) reset_timer() {
	var ms int64 = 50
	if rf.state != LEADER {
		ms += rand.Int63()%128 + 50
	}
	logger.Log(logger.DTimer, "S%d: Timer reseted for %d ms", rf.me, ms)
	rf.timer = time.NewTimer(time.Millisecond * time.Duration(ms))
}

type RaftUpdateOption func(*Raft)
type RaftGetIntOption func(*Raft) int

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

func rfUpdateAddCurTerm(adder int) RaftUpdateOption {
	return func(rf *Raft) {
		rf.currentTerm += adder
	}
}

func rfUpdateVotedFor(voted int) RaftUpdateOption {
	return func(rf *Raft) {
		rf.votedFor = voted
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

	rf_metadata := rf.getInt(rfGetCurTerm(), rfGetMe(), rfGetVoteFor())
	cur_term := rf_metadata[0]
	me := rf_metadata[1]
	vote_for := rf_metadata[2]

	if require_term > cur_term {
		rf.update(rfUpdateCurTerm(require_term), rfUpdateState(FOLLOWER), rfUpdateVotedFor(to_vote), rfUpdateTimer())

		reply.Term = cur_term
		reply.VoteGranted = true

		logger.Log(logger.DVote, "S%d <- S%d: Got vote", to_vote, me)
	} else if vote_for == -1 || vote_for == to_vote {
		rf.update(rfUpdateState(FOLLOWER), rfUpdateTimer())

		reply.Term = cur_term
		reply.VoteGranted = true

		logger.Log(logger.DVote, "S%d <- S%d: Got vote", to_vote, me)
	} else {
		reply.Term = cur_term

		if require_term < cur_term {
			logger.Log(logger.DInfo, "S%d </ S%d: Don't vote, out of date. cur term: %d, request term: %d", to_vote, me, cur_term, require_term)
		} else if vote_for != to_vote {
			logger.Log(logger.DInfo, "S%d </ S%d: Don't vote, have voted to S%d", to_vote, me, vote_for)
		}

		reply.VoteGranted = false
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
	logger.Log(logger.DVote, "S%d -> S%d: Request vote", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) tryLeader() bool {
	rf_metadata := rf.getInt(rfGetMe(), rfGetCurTerm())
	me := rf_metadata[0]
	cur_term := rf_metadata[1] + 1
	args := RequestVoteArgs{Term: cur_term, CandidateID: me}

	rf.update(rfUpdateCurTerm(cur_term), rfUpdateState(CANDIDATE), rfUpdateVotedFor(me))
	connected_num := 1
	voteGranted_num := 1
	reply_chan := make(chan int, len(rf.peers))

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.requestVoteFromPeer(server, args, reply_chan)
	}

	timer := time.NewTimer(TIMEOUT_LIMIT)
	defer timer.Stop()

	return rf.collectVotes(reply_chan, timer, connected_num, voteGranted_num, me)
}

func (rf *Raft) requestVoteFromPeer(server int, args RequestVoteArgs, reply_chan chan int) {
	cur_term, cur_state := rf.GetDetailState()
	if cur_state == FOLLOWER {
		reply_chan <- -1
		return
	}

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

func (rf *Raft) checkVotes(voteGranted_num, connected_num, me int) bool {
	logger.Log(logger.DVote, "S%d: Recv vote %d in %d", me, voteGranted_num, connected_num)
	_, cur_state := rf.GetDetailState()

	if connected_num > 1 && voteGranted_num >= (connected_num+2)/2 && cur_state == CANDIDATE {
		rf.update(rfUpdateState(LEADER))
		logger.Log(logger.DLeader, "S%d: Become leader", me)
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

	rf_metadata := rf.getInt(rfGetCurTerm(), rfGetMe())
	cur_term := rf_metadata[0]
	me := rf_metadata[1]

	if leader_term < cur_term {
		reply.Term = cur_term
		reply.Success = false
		logger.Log(logger.DAppend, "S%d <- S%d: Leader out of date", leader_id, me)
	} else {
		rf.update(rfUpdateVotedFor(leader_id), rfUpdateTimer(), rfUpdateCurTerm(leader_term), rfUpdateState(FOLLOWER))

		reply.Term = leader_term
		reply.Success = true
		logger.Log(logger.DAppend, "S%d <- S%d: Update status successfully", leader_id, me)
	}
}

func (rf *Raft) sendAppend(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	logger.Log(logger.DAppend, "S%d -> S%d: Send append entries", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppends() {

	rf_metadata := rf.getInt(rfGetCurTerm(), rfGetMe())
	cur_term := rf_metadata[0]
	me := rf_metadata[1]
	args := AppendEntriesArgs{Term: cur_term, LeaderID: me}

	connected_num := len(rf.peers) - 1
	for server := range rf.peers {
		if server == me {
			continue
		}

		_, is_leader := rf.GetState()
		if !is_leader {
			break
		}

		reply := AppendEntriesReply{}
		resultCh := make(chan bool, 1)

		go func() {
			ok := rf.sendAppend(server, &args, &reply)
			resultCh <- ok
		}()

		select {
		case <-resultCh:
			{
				if !reply.Success {
					if reply.Term > cur_term {
						logger.Log(logger.DFollower, "S%d: out-of-date, become follower. Self: %d, reply: %d", me, cur_term, reply.Term)
						rf.update(rfUpdateCurTerm(reply.Term), rfUpdateState(FOLLOWER))
					} else {
						logger.Log(logger.DAppend, "S%d: Update S%d error", me, server)
					}
				}
			}
		case <-time.After(TIMEOUT_LIMIT):
			{
				connected_num -= 1
				logger.Log(logger.DWarn, "S%d: Send append entries time-out", me)
			}
		}

	}
	logger.Log(logger.DAppend, "S%d: Append entries send end", me)
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

	rf_metadata := rf.getInt(rfGetCommID(), rfGetCurTerm(), rfGetState())
	index := rf_metadata[0] + 1
	term := rf_metadata[1]
	isLeader := rf_metadata[2] == LEADER
	logger.Log(logger.DInfo, "S%d: get start call", rf.me)

	if isLeader {
		go rf.sendAppends()
	}
	// Your code here (3B).

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
				rf.sendAppends()
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
	rf := &Raft{timer: time.NewTimer(time.Duration(50+(rand.Int63()%300)) * time.Millisecond)}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
