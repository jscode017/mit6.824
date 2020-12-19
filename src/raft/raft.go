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
	"../labrpc"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

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
type AppendEntriesArgs struct {
	IsHeartBeat bool
	Term        int
}
type AppendEntriesReply struct {
	OutDated bool
	Term     int
}

//enum for leader state
const (
	Follower = iota
	Candidate
	Leader
)

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
	CurTerm            int
	LeaderState        int
	VotedTerm          int
	VotedFor           int
	LastLogIndex       int
	LastLogTerm        int
	ReqVoteReplych     chan RequestVoteReply
	AppendEntriesCh    chan AppendEntriesArgs
	HeartBeatCh        chan bool
	VoteForNewLeaderCh chan bool

	leaderElecTicker  *time.Ticker
	minLeaderElecTime int
	maxLeaderElecTime int

	//if just vote or get heart beat and follower trigger election timer, should just break
	justGetHeartBeat bool
	justVote         bool
}

func (rf *Raft) GetLastLogIndex() int {
	return rf.LastLogIndex
}
func (rf *Raft) GetLastLogTerm() int {
	return rf.LastLogTerm
}
func (rf *Raft) Add1toTerm() {
	rf.CurTerm++
}
func (rf *Raft) UpdateTerm(newTerm int) {
	rf.CurTerm = newTerm
}

// return currentTerm and whether this server
// believes it is the leader.
//since contains lock, may not be a good idea to call from other functions in raft.go
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.GetTerm()
	isleader = rf.CheckIsLeader()
	return term, isleader
}
func (rf *Raft) CheckIsLeader() bool {
	return rf.GetLeaderState() == Leader
}
func (rf *Raft) GetLeaderState() int {
	return rf.LeaderState
}
func (rf *Raft) GetTerm() int {
	return rf.CurTerm
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	Me          int
}

func MakeRequestVoteArgs(term, candidateID, lastLogTerm, lastLogIndex int) RequestVoteArgs {
	return RequestVoteArgs{
		Term:         term,
		CandidateID:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendHeartBeats(heartBeat *AppendEntriesArgs, heartBeatOutDatedCh chan int) {
	for i := 0; i < len(rf.peers); i++ {
		if rf.killed() {
			return
		}
		if i == rf.me {
			continue
		}
		serverID := i
		go func(serverID int, heartBeatOutDatedCh chan int) {

			heartBeatReply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(serverID, heartBeat, &heartBeatReply)
			if ok && heartBeatReply.OutDated {
				heartBeatOutDatedCh <- heartBeatReply.Term
			}
			if ok {
				log.Printf("raft %d send heartbeat to %d\n", rf.me, serverID)
			}
		}(serverID, heartBeatOutDatedCh)
	}
}

//Append entries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.IsHeartBeat {
		ok := rf.DealWithHeartBeat(args, reply)
		if ok {
			rf.justGetHeartBeat = true
			rf.HeartBeatCh <- true
		}
		return
	}
	rf.DealWithAppendEntries(args)
}

func (rf *Raft) DealWithHeartBeat(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	curTerm := rf.GetTerm()
	heartBeatTerm := args.Term
	if curTerm == heartBeatTerm {
		return true
	} else if curTerm > heartBeatTerm {
		reply.OutDated = true
		reply.Term = curTerm
		return false
	} else { //<
		rf.UpdateTerm(heartBeatTerm)
		return true
	}
}
func (rf *Raft) DealWithAppendEntries(Args *AppendEntriesArgs) {
	log.Fatal("should not enter deal with append entry in lab2A")
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.CheckReqVoteValid(args, reply) {
		return
	}
	rf.Vote(args.CandidateID, args.Term, reply)
	rf.justVote = true
	rf.VoteForNewLeaderCh <- true
	log.Printf("%d voted for %d at time %s\n", rf.me, args.CandidateID, time.Now().String())
}
func (rf *Raft) Vote(voteForID, voteTerm int, reply *RequestVoteReply) {
	rf.VotedTerm = rf.CurTerm
	rf.VotedFor = voteTerm
	reply.VoteGranted = true
	reply.Me = rf.me
}
func (rf *Raft) CheckReqVoteValid(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if !rf.CheckReqVoteTerm(args, reply) {
		return false
	}
	if !rf.CheckLastLog(args, reply) {
		return false
	}
	return true
}
func (rf *Raft) CheckLastLog(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return true
}
func (rf *Raft) CheckReqVoteTerm(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	curTerm := rf.GetTerm()
	reqVoteTerm := args.Term
	if rf.VotedTerm > reqVoteTerm {
		reply.VoteGranted = false
		return false
	} else if rf.VotedTerm == reqVoteTerm && rf.VotedFor != -1 {
		reply.VoteGranted = false
		return false
	}
	if curTerm > reqVoteTerm {
		reply.Term = curTerm
		reply.VoteGranted = false
		return false
	} else if curTerm < reqVoteTerm {
		rf.UpdateTerm(reqVoteTerm)
	}
	log.Printf("raft %d log granted prev vote term %d votefor %d\n", rf.me, rf.VotedTerm, rf.VotedFor)
	return true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.GetTerm()
	isLeader := rf.CheckIsLeader()

	// Your code here (2B).

	return index, term, isLeader
}
func (rf *Raft) ChangeLeaderState(newLeaderState int) {
	rf.LeaderState = newLeaderState
}

//run is than main loop for raft
func (rf *Raft) Run() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		leaderState := rf.GetLeaderState()
		rf.mu.Unlock()
		switch leaderState {
		case Follower:
			rf.Follower()
		case Candidate:
			rf.Candidate()
		case Leader:
			rf.Leader()
		}
	}
}
func (rf *Raft) StepDown() {
	rf.LeaderState = Follower
}
func (rf *Raft) Follower() {
	for {
		if rf.killed() {
			return
		}
		randTime := getRandTime(rf.minLeaderElecTime, rf.maxLeaderElecTime)

		/*rf.mu.Lock()
		prevTerm := rf.GetTerm()
		rf.mu.Unlock()*/
		select {
		case <-time.After(time.Duration(randTime) * time.Millisecond):
			rf.mu.Lock()
			if rf.justVote || rf.justGetHeartBeat {
				rf.mu.Unlock()
				break
			}
			rf.Add1toTerm()
			if !rf.CanEnterCandidate() {
				rf.mu.Unlock()
				break
			}
			rf.ChangeLeaderState(Candidate)
			rf.VotedTerm = rf.GetTerm()
			rf.VotedFor = rf.me
			log.Printf("rafts %d change to candidate at term %d and voteterm %d voted for %d at time %s\n", rf.me, rf.GetTerm(), rf.VotedTerm, rf.VotedFor, time.Now().String())
			rf.mu.Unlock()
			return

		case <-rf.VoteForNewLeaderCh:
			rf.mu.Lock()
			rf.justVote = false
			rf.mu.Unlock()
			break
		case <-rf.HeartBeatCh:
			rf.mu.Lock()
			rf.justGetHeartBeat = false
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) CanEnterCandidate() bool {
	return !rf.killed()
}
func (rf *Raft) Candidate() {
	//the validity for candidate state should be checked before enter candidate state

	var voteGot int32
	voteGot = 1
	rf.mu.Lock()

	majority := int(math.Ceil(float64(len(rf.peers)) / 2.0))
	log.Printf("raft %d prepare to send out req vote\n", rf.me)
	rf.mu.Unlock()
	wg := sync.WaitGroup{}

	if rf.killed() {
		return
	}
	rf.mu.Lock()
	reqVoteArgs := MakeRequestVoteArgs(rf.GetTerm(), rf.me, rf.GetLastLogTerm(), rf.GetLastLogIndex())
	peersNum := len(rf.peers)
	rf.mu.Unlock()
	voteExceedMajorCh := make(chan bool)
	for i := 0; i < peersNum; i++ {
		if i == rf.me {
			continue
		}
		serverID := i
		wg.Add(1)
		go func(serverID int) {
			reqVoteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverID, &reqVoteArgs, &reqVoteReply)
			if !ok {
				log.Printf("server %d not responding\n", serverID)
				wg.Done()
				return
			}
			rf.mu.Lock()
			if rf.DealWithReqVoteReply(&reqVoteReply) {
				log.Printf("got vote from %d\n", reqVoteReply.Me)
				newVoteGot := atomic.AddInt32(&voteGot, 1)
				if newVoteGot == int32(majority) {
					voteExceedMajorCh <- true
				}
			}
			rf.mu.Unlock()
			wg.Done()
			return
		}(serverID)
	}

	electionClosedCh := make(chan bool)
	go func() {
		wg.Wait()
		if voteGot < int32(majority) {
			electionClosedCh <- true
		}
		return
	}()
	candidateElecTimeOut := rf.minLeaderElecTime
	select {
	case <-time.After(time.Duration(candidateElecTimeOut) * time.Millisecond):
		rf.mu.Lock()
		rf.StepDown()
		rf.mu.Unlock()
		return
	case <-electionClosedCh:
		rf.mu.Lock()
		rf.StepDown()
		rf.mu.Unlock()
		return
	case <-voteExceedMajorCh:
		rf.mu.Lock()
		rf.ChangeLeaderState(Leader)
		log.Printf("raft %d becomes leader at term %d got %d votes out of %d\n", rf.me, rf.GetTerm(), voteGot, len(rf.peers))
		rf.mu.Unlock()
		return
	case <-rf.VoteForNewLeaderCh:
		rf.mu.Lock()
		rf.StepDown()
		rf.mu.Unlock()
		return
	case <-rf.HeartBeatCh:
		rf.mu.Lock()
		rf.StepDown()
		rf.mu.Unlock()
		return
	}
}
func (rf *Raft) DealWithReqVoteReply(reply *RequestVoteReply) bool {
	if reply.VoteGranted {
		return true
	} else {
		if reply.Term > rf.CurTerm {
			rf.UpdateTerm(reply.Term)
			rf.StepDown()
		}
		return false
	}
}

func (rf *Raft) Leader() {
	rf.mu.Lock()
	heartBeat := AppendEntriesArgs{
		IsHeartBeat: true,
		Term:        rf.GetTerm(), //this may or may not be dangerous
	}
	rf.mu.Unlock()
	heartBeatOutDatedCh := make(chan int)

	go rf.sendHeartBeats(&heartBeat, heartBeatOutDatedCh)

	//majorityLostCh := make(chan bool)

	for {
		if rf.killed() {
			return
		}

		select {
		case <-time.After(150 * time.Millisecond):
			//var lostPeers int64
			//lostPeers = 0
			for i := 0; i < len(rf.peers); i++ {
				if rf.killed() {
					return
				}
				if i == rf.me {
					continue
				}
				serverID := i
				go func(serverID int) {
					heartBeatReply := AppendEntriesReply{}
					rf.sendAppendEntries(serverID, &heartBeat, &heartBeatReply)
					if heartBeatReply.OutDated {
						heartBeatOutDatedCh <- heartBeatReply.Term
					}
				}(serverID)
			}
		case <-rf.VoteForNewLeaderCh:
			rf.mu.Lock()
			log.Println("leader steps down due to vote for new")
			rf.StepDown()
			rf.mu.Unlock()
			return
		case <-rf.HeartBeatCh:
			rf.mu.Lock()
			log.Println("leader step down due to heart beat")
			rf.StepDown()
			rf.mu.Unlock()
			return
		case term := <-heartBeatOutDatedCh:
			rf.mu.Lock()
			log.Println("leader step down due to heart beat out of date")
			rf.UpdateTerm(term)
			rf.StepDown()
			rf.mu.Unlock()

			return
		}
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.AppendEntriesCh = make(chan AppendEntriesArgs)
	rf.CurTerm = 0
	rf.HeartBeatCh = make(chan bool)
	rf.LastLogTerm = 0
	rf.LastLogIndex = 0
	rf.LeaderState = Follower
	rf.ReqVoteReplych = make(chan RequestVoteReply)
	rf.VoteForNewLeaderCh = make(chan bool)
	rf.VotedFor = -1
	rf.VotedTerm = -1

	rf.minLeaderElecTime = 300
	rf.maxLeaderElecTime = 600

	rf.justGetHeartBeat = false
	rf.justVote = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Run()

	return rf
}
