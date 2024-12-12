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

type LogEntry struct {
	Term     int
	Command  interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all severs
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex   []int
	matchIndex  []int

	// other volatile state
	state         ServerState
	lastHeartbeat time.Time
	cond          *sync.Cond
	applyCh       chan ApplyMsg
}

type ServerState int
const (
	Leader ServerState = iota
	Follower
	Candidate
)

const heartbeat = 150
const electionTimeout = 500

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == Leader

	// Your code here (3A).
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term         int
	Success      bool

	// Optimization: send inconsistent log entry to reduce rejections
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply false if candidate's Term T < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// If RPC contains Term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.fastForward(args.Term)
	}
	// candidate's Term T == currentTerm
	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
}

// determines whether the candidate’s log is at least as up-to-date as receiver’s log
func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	logIndex := len(rf.log) - 1
	logTerm  := rf.log[logIndex].Term
	return (args.LastLogTerm > logTerm) || (args.LastLogTerm == logTerm && args.LastLogIndex >= logIndex)
}

// set the current term, convert to follower state
// the caller is responsible for holding the mutex
func (rf *Raft) fastForward(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Reset election timer when receive RPC from the current viable leader
	rf.lastHeartbeat = time.Now()

	// If any RPC contains Term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.fastForward(args.Term)
	}
	reply.Term = rf.currentTerm

	// If log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm, reply false
	if args.PrevLogIndex >= len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		rf.rejectAppendEntries(args, reply)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	i, j := 0, args.PrevLogIndex + 1
	for i < len(args.Entries) && j < len(rf.log) {
		if args.Entries[i].Term == rf.log[j].Term {
			i++
			j++
		} else {
			rf.log = rf.log[:j]
			break
		}
	}
	// Append any new entries not already in the log
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
	}
	reply.Success = true

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
		rf.cond.Broadcast()
	}
}

func (rf *Raft) rejectAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.LastLogIndex = len(rf.log) - 1
		reply.LastLogTerm = rf.log[reply.LastLogIndex].Term
	} else {
		index := args.PrevLogIndex
		term  := rf.log[index].Term
		for index > 0 && rf.log[index-1].Term == term {
			index--
		}
		reply.Success = false
		reply.LastLogIndex = index
		reply.LastLogTerm = term
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// Your code here (3B).
	if !rf.killed() && isLeader {
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
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
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.lastHeartbeat) > electionTimeout * time.Millisecond {
			go rf.startElection()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// If a follower receives no communication over an election timeout period,
// then it assumes there is no live leader and begins an election.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()

	idx := len(rf.log) - 1
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: idx, LastLogTerm: rf.log[idx].Term}

	votes := 0
	for server := range rf.peers {
		if server == rf.me {
			votes++
			continue
		}

		// send a RequestVote RPC to each peer, and count the votes it receives
		go func(server int) {
			response := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &response)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if response.VoteGranted {
				votes++
			} else if response.Term > rf.currentTerm {
				rf.fastForward(response.Term)
			}
			if rf.state == Candidate && votes * 2 > len(rf.peers) {
				rf.establishLeader()
			}
		}(server)
	}
}

func (rf *Raft) establishLeader() {
	rf.state = Leader
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	go rf.leaderHeartbeat()
	go rf.leaderLogEntries()
	go rf.updateCommitIndex()
}

// A leader sends periodic heartbeats to all followers
// through AppendEntries RPC containing no log entry.
func (rf *Raft) leaderHeartbeat() {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.log) - 1,
			PrevLogTerm:  rf.log[len(rf.log)-1].Term,
			Entries:      make([]LogEntry, 0),
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				response := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &response)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if response.Term > rf.currentTerm {
						rf.fastForward(response.Term)
					}
				}
			}(server)
		}

		time.Sleep(heartbeat * time.Millisecond)
	}
}

// A leader issues AppendEntries RPC to each follower
// to replicate its log entries.
func (rf *Raft) leaderLogEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendLogEntries(i)
	}
}

func (rf *Raft) sendLogEntries(server int) {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		// If the leader's last log index >= nextIndex for a follower:
		// send AppendEntries RPC with log entries starting at nextIndex
		last := len(rf.log) - 1
		next := rf.nextIndex[server]
		if last >= next {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: next - 1,
				PrevLogTerm:  rf.log[next-1].Term,
				Entries:      rf.log[next:],
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok {
				rf.handleAppendEntriesReply(server, &args, &reply)
			}
		} else {
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.fastForward(reply.Term)
		return
	}

	// If successful: update nextIndex and matchIndex for follower
	// Otherwise, decrement nextIndex to bypass all conflicting entries and retry
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.LastLogIndex < rf.nextIndex[server] {
		rf.nextIndex[server] = reply.LastLogIndex
	} else {
		rf.nextIndex[server] = max(rf.nextIndex[server] - 1, 1)
	}
}

// A server periodically checks the latest commit index.
// If commitIndex > lastApplied, increment lastApplied, and log[lastApplied] to state machine.
func (rf *Raft) commitLogEntries() {
	for {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.cond.Wait()
		}
		rf.lastApplied++
		index := rf.lastApplied
		command := rf.log[index].Command
		rf.mu.Unlock()

		msg := ApplyMsg{CommandValid: true, Command: command, CommandIndex: index}
		rf.applyCh <- msg
	}
}

// The leader updates the commit index if there exists an N such that N > commitIndex,
// a majority of matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N.
func (rf *Raft) updateCommitIndex() {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		majority := 1
		if rf.commitIndex < len(rf.log) - 1 {
			n := rf.commitIndex + 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					majority++
				}
			}
			if majority * 2 > len(rf.peers) {
				rf.commitIndex = n
				rf.cond.Broadcast()
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower
	rf.lastHeartbeat = time.Now()
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commitLogEntries()

	return rf
}
