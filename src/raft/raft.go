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

	PrevLogIndex int
	PrevLogTerm  int
	EntryLogs    []EntryLog
	LeaderCommit int
}
type AppendEntriesReply struct {
	OutDated bool
	Term     int
	Success  bool
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
	CurTerm      int
	LeaderState  int
	VotedFor     int
	LastLogIndex int
	LastLogTerm  int

	AppendEntriesCh    chan AppendEntriesArgs
	HeartBeatCh        chan bool
	VoteForNewLeaderCh chan bool

	minLeaderElecTime int
	maxLeaderElecTime int

	//if just vote or get heart beat and follower trigger election timer, should just break
	justGetHeartBeat bool
	justVote         bool

	entryLogs   []EntryLog
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

	nextIndex  []int
	matchIndex []int

	SendEntryCh chan EntryLog
}
type EntryLog struct {
	Command interface{}
	Term    int
	Index   int
}

func (rf *Raft) GetLastLogIndex() int {
	return len(rf.entryLogs)
}
func (rf *Raft) GetLastLogTerm() int {
	if len(rf.entryLogs) == 0 {
		return 0
	}
	return rf.entryLogs[len(rf.entryLogs)-1].Term
}
func (rf *Raft) Add1toTerm() {
	rf.UpdateTerm(rf.GetTerm() + 1)
}

//need to set votedfor==-1 when updating term
func (rf *Raft) UpdateTerm(newTerm int) {
	rf.CurTerm = newTerm
	rf.VotedFor = -1
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

func (rf *Raft) GetLeaderCommit() int {
	return rf.commitIndex
}
func (rf *Raft) Add1ToCommitIndex() {
	rf.UpdateCommitIndex(rf.GetLeaderCommit() + 1)
}
func (rf *Raft) UpdateCommitIndex(newCommitIndex int) {
	rf.commitIndex = newCommitIndex
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

func (rf *Raft) sendHeartBeats(originHeartBeat *AppendEntriesArgs, heartBeatOutDatedCh chan int) {
	for i := 0; i < len(rf.peers); i++ {
		if rf.killed() {
			return
		}
		if i == rf.me {
			continue
		}
		serverID := i
		go func(serverID int, heartBeatOutDatedCh chan int) {
			for {
				rf.mu.Lock()
				if !rf.CheckIsLeader() {
					rf.mu.Unlock()
					return
				}
				heartBeat := &AppendEntriesArgs{
					LeaderCommit: rf.GetLeaderCommit(),
					PrevLogIndex: rf.GetLastLogIndex(),
					PrevLogTerm:  rf.GetLastLogTerm(),
					Term:         rf.GetTerm(),
				}
				//log.Printf("sending heartbeat to %d\n", serverID)
				nextIndex := rf.nextIndex[serverID]
				if rf.GetLastLogIndex() >= nextIndex {
					heartBeat.EntryLogs = rf.entryLogs[nextIndex-1:]
					heartBeat.PrevLogIndex = rf.nextIndex[serverID] - 1
					if heartBeat.PrevLogIndex > 0 {
						heartBeat.PrevLogTerm = rf.entryLogs[heartBeat.PrevLogIndex-1].Term
					} else {
						heartBeat.PrevLogTerm = 0
					}
					heartBeat.LeaderCommit = rf.GetLeaderCommit()
				} else {
					heartBeat.EntryLogs = []EntryLog{}
				}
				rf.mu.Unlock()
				heartBeatReply := AppendEntriesReply{}
				//log.Printf("sending heart beat to %d\n", serverID)
				ok := rf.sendAppendEntries(serverID, heartBeat, &heartBeatReply)
				//log.Printf("get result for heartbeat to %d\n", serverID)
				if !ok {
					//log.Printf("server %d not replying\n", serverID)
					return
				}
				if heartBeatReply.OutDated {
					log.Println("heartbeat outdated")
					heartBeatOutDatedCh <- heartBeatReply.Term
					return
				}
				/*type AppendEntriesReply struct {
					OutDated bool
					Term     int
					Success  bool
				}*/

				if !heartBeatReply.Success {
					rf.mu.Lock()
					log.Println("heartbeat insuccess")
					rf.nextIndex[serverID]--
					rf.mu.Unlock()
					continue
				}

				//success
				if heartBeatReply.Success {
					//log.Println("heartbeat success")
					majority := int(math.Ceil(float64(len(rf.peers)) / 2.0))

					rf.mu.Lock()

					originalMatchIndex := rf.matchIndex[serverID]
					if len(heartBeat.EntryLogs) > 0 && originalMatchIndex < heartBeat.EntryLogs[len(heartBeat.EntryLogs)-1].Index {

						rf.matchIndex[serverID] = heartBeat.EntryLogs[len(heartBeat.EntryLogs)-1].Index
						rf.nextIndex[serverID] = rf.matchIndex[serverID] + 1
						for i := 0; i < len(heartBeat.EntryLogs); i++ {
							entryIndex := heartBeat.EntryLogs[i].Index
							cnt := 1
							for j := 0; j < len(rf.peers); j++ {
								if j == rf.me {
									continue
								}
								if rf.matchIndex[j] >= entryIndex {
									cnt++
								}
							}
							if cnt >= majority {
								rf.commitIndex = intMax(entryIndex, rf.commitIndex)
								log.Printf("commit index of leader is %d\n", rf.commitIndex)
							} else {
								break
							}
						}

						rf.commit()
					}
					rf.mu.Unlock()
					return
				}
			}
		}(serverID, heartBeatOutDatedCh)
	}
}

func (rf *Raft) commit() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		log.Printf("raft %d commiting index %d\n", rf.me, rf.lastApplied)
		applyMsg := ApplyMsg{
			Command:      rf.entryLogs[rf.lastApplied-1].Command,
			CommandIndex: rf.lastApplied,
			CommandValid: true,
		}
		rf.applyCh <- applyMsg
	}
}

//Append entries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("raft %d %+v\n", rf.me, args)

	isHeartBeat := len(args.EntryLogs) == 0
	if isHeartBeat {
		log.Println("is heartbeat")
		ok := rf.DealWithHeartBeat(args, reply)
		if ok {
			log.Printf("raft %d got heart beat and ok\n", rf.me)
			reply.Success = true
			//rf.justGetHeartBeat = true
			//rf.HeartBeatCh <- true
			rf.commitIndex = intMax(rf.commitIndex, args.LeaderCommit)
			rf.commitIndex = intMin(rf.commitIndex, len(rf.entryLogs))
			log.Printf("raft %d ready to enter commit\n", rf.me)
			rf.commit()
		}
		return
	}
	ok := rf.DealWithAppendEntries(args, reply)
	if ok {
		reply.Success = true
		rf.commit()
	}
	log.Println("end of append entry")
}

func (rf *Raft) DealWithHeartBeat(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	curTerm := rf.GetTerm()
	heartBeatTerm := args.Term
	reply.Term = curTerm
	reply.OutDated = false
	if curTerm > heartBeatTerm {
		log.Printf("raft %d got term %d have term %d\n", rf.me, heartBeatTerm, curTerm)
		reply.OutDated = true
		return false
	} else if curTerm < heartBeatTerm { //<
		rf.UpdateTerm(heartBeatTerm)
	}
	rf.HeartBeatCh <- true
	rf.justGetHeartBeat = true
	if args.PrevLogIndex > len(rf.entryLogs) {
		log.Printf("raft %d got heart beat(empty logs) prev entrylog not match\n", rf.me)
		reply.Success = false
		return false
	}
	prevIndex := args.PrevLogIndex
	if prevIndex > 0 && rf.entryLogs[prevIndex-1].Term != args.PrevLogTerm {
		log.Printf("raft %d got heart beat(empty logs) previndex term not match\n", rf.me)
		reply.Success = false
		return false
	}
	return true
}
func (rf *Raft) DealWithAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	curTerm := rf.GetTerm()
	heartBeatTerm := args.Term
	reply.Term = curTerm
	if heartBeatTerm > curTerm {
		log.Println("updating term")
		rf.UpdateTerm(heartBeatTerm)
	} else if heartBeatTerm < curTerm {
		log.Printf("raft %d recv heart beat but outdated\n", rf.me)
		reply.OutDated = true
		reply.Success = false
		return false
	}
	rf.HeartBeatCh <- true
	rf.justGetHeartBeat = true
	if args.PrevLogIndex > len(rf.entryLogs) {
		log.Printf("raft %d got heart beat prev entrylog not match\n", rf.me)
		reply.Success = false
		return false
	}
	prevIndex := args.PrevLogIndex
	if prevIndex > 0 && rf.entryLogs[prevIndex-1].Term != args.PrevLogTerm {
		log.Printf("raft %d got heart beat previndex term not match\n", rf.me)
		reply.Success = false
		return false
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = intMin(args.LeaderCommit, args.EntryLogs[len(args.EntryLogs)-1].Index)
	}

	entryLogLastIndex := args.EntryLogs[len(args.EntryLogs)-1].Index
	alreadyInLog := entryLogLastIndex <= len(rf.entryLogs) && rf.entryLogs[entryLogLastIndex-1].Term == args.EntryLogs[len(args.EntryLogs)-1].Term
	if alreadyInLog {
		log.Printf("raft %d got heart beat, success but alreay in log\n", rf.me)
		reply.Success = true
		return true
	}
	startIndex := args.EntryLogs[0].Index
	for i := 0; i < len(args.EntryLogs); i++ {
		if i+startIndex <= len(rf.entryLogs) {
			rf.entryLogs[i+startIndex-1] = args.EntryLogs[i]
		} else {
			rf.AppendEntryLog(args.EntryLogs[i])
		}
	}
	lastAppendEntry := args.EntryLogs[len(args.EntryLogs)-1]
	rf.entryLogs = rf.entryLogs[:lastAppendEntry.Index]
	log.Printf("raft %d's new log %+v\n", rf.me, rf.entryLogs)
	reply.Success = true
	return true
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
	rf.VotedFor = voteTerm
	reply.VoteGranted = true
	reply.Me = rf.me
}
func (rf *Raft) CheckReqVoteValid(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if !rf.CheckReqVoteTerm(args, reply) {
		log.Printf("raft %d recv req vote but insuccess due to term\n", rf.me)
		return false
	}
	if !rf.CheckLastLog(args, reply) {
		log.Printf("raft %d recv req vote but insuccess due to last log\n", rf.me)
		return false
	}
	return true
}
func (rf *Raft) CheckLastLog(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DO NOT MERGE THIS 2 IFS, SINCE IT MAY BE POSSIBLE THAT An ENTRY HAS LARGER INDEX BUT SMALLER TERM
	lastLogTerm := rf.GetLastLogTerm()
	lastLogIndex := rf.GetLastLogIndex()
	if lastLogTerm < args.LastLogTerm {
		return true
	} else if lastLogTerm == args.LastLogTerm {
		if lastLogIndex <= args.LastLogIndex {
			return true
		}
		return false
	}

	//logTerm>args.LogTerm
	return false
}
func (rf *Raft) CheckReqVoteTerm(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	curTerm := rf.GetTerm()
	reply.Term = curTerm
	reqVoteTerm := args.Term
	if curTerm > reqVoteTerm {
		reply.VoteGranted = false
		log.Printf("raft %d get req vote but current term>req vote term\n", rf.me)
		return false
	} else if curTerm < reqVoteTerm {
		rf.UpdateTerm(reqVoteTerm)
	} else { //==
		//reply.VoteGranted = (rf.VotedFor == -1)
		return rf.VotedFor == -1
	}
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

func (rf *Raft) sendRequestVotes(wg *sync.WaitGroup, voteExceedMajorCh chan bool, majority int, electionClosedCh chan bool, reqVoteArgs RequestVoteArgs) {
	var voteGot int32
	voteGot = 1
	rf.mu.Lock()
	peersNum := len(rf.peers)
	rf.mu.Unlock()
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
				log.Printf("req vote to server %d not responding\n", serverID)
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
			} else {
				log.Printf("does not get req vote of %d\n", serverID)
			}
			rf.mu.Unlock()
			wg.Done()
			return
		}(serverID)
	}
	go func() {
		wg.Wait()
		log.Println("get all votes")
		if voteGot < int32(majority) {
			electionClosedCh <- true
		}
		return
	}()
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

	// Your code here (2B).
	index := rf.GetLastLogIndex() + 1
	term := rf.GetTerm()
	isLeader := rf.CheckIsLeader()

	entryLog := EntryLog{
		Command: command,
		Index:   index,
		Term:    term,
	}
	//rf.mu.Lock()
	if isLeader {
		rf.AppendEntryLog(entryLog)
		log.Printf("raft %d get entrylogs %+v\n", rf.me, rf.entryLogs)
	} //rf.mu.Unlock()
	//rf.SendEntryCh <- entryLog
	return index, term, isLeader
}
func (rf *Raft) AppendEntryLog(entryLog EntryLog) {
	rf.entryLogs = append(rf.entryLogs, entryLog)
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
			log.Printf("raft %d is follower\n", rf.me)
			rf.Follower()
			break
		case Candidate:
			rf.Candidate()
			break
		case Leader:
			rf.Leader()
			break
		}
	}
}
func (rf *Raft) StepDown() {
	rf.LeaderState = Follower
}
func (rf *Raft) Follower() {
	for {
		if rf.killed() {
			log.Printf("raft %d killed\n", rf.me)
			return
		}
		randTime := getRandTime(rf.minLeaderElecTime, rf.maxLeaderElecTime)

		/*rf.mu.Lock()
		prevTerm := rf.GetTerm()
		rf.mu.Unlock()*/
		//log.Printf("raft %d in followe loop\n", rf.me)
		select {
		case <-time.After(time.Duration(randTime) * time.Millisecond):
			log.Printf("raft %d election time out\n", rf.me)
			rf.mu.Lock()
			justVotedOrJustGetHeartBeat := (rf.justVote || rf.justGetHeartBeat)
			if justVotedOrJustGetHeartBeat {
				log.Printf("raft %d just get heartbeat or voted\n", rf.me)
				rf.mu.Unlock()
				break
			}
			rf.Add1toTerm()
			if !rf.CanEnterCandidate() {
				log.Printf("raft %d can not enter candidate\n", rf.me)
				rf.mu.Unlock()
				break
			}
			rf.ChangeLeaderState(Candidate)
			rf.VotedFor = rf.me
			log.Printf("rafts %d change to candidate at term %d at time %s\n", rf.me, rf.GetTerm(), time.Now().String())
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
	rf.mu.Unlock()
	voteExceedMajorCh := make(chan bool)
	electionClosedCh := make(chan bool)
	rf.sendRequestVotes(&wg, voteExceedMajorCh, majority, electionClosedCh, reqVoteArgs)
	candidateElecTime := rf.minLeaderElecTime
	select {
	case <-time.After(time.Duration(candidateElecTime) * time.Millisecond):
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
		log.Printf("raft %d becomes leader at term %d\n", rf.me, rf.GetTerm())
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
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.GetLastLogIndex() + 1
	}
	heartBeat := AppendEntriesArgs{
		IsHeartBeat:  true,
		Term:         rf.GetTerm(), //this may or may not be dangerous
		LeaderCommit: rf.GetLeaderCommit(),
	}
	rf.mu.Unlock()
	heartBeatOutDatedCh := make(chan int)

	log.Printf("raft %d sending hearbeats at term %d\n", rf.me, heartBeat.Term)
	go rf.sendHeartBeats(&heartBeat, heartBeatOutDatedCh)

	//majorityLostCh := make(chan bool)

	for {
		//log.Println("leader loop")
		if rf.killed() {
			return
		}

		select {
		case <-time.After(150 * time.Millisecond):
			//log.Println("send heart beat")
			rf.sendHeartBeats(&heartBeat, heartBeatOutDatedCh)
		case <-rf.VoteForNewLeaderCh:
			rf.mu.Lock()
			log.Printf("leader %d steps down due to vote for new\n", rf.me)
			rf.StepDown()
			rf.mu.Unlock()
			return
		case <-rf.HeartBeatCh:
			rf.mu.Lock()
			log.Printf("leader %d step down due to heart beat\n", rf.me)
			rf.StepDown()
			rf.mu.Unlock()
			//time.Sleep(50 * time.Millisecond)
			return
		case term := <-heartBeatOutDatedCh:
			rf.mu.Lock()
			log.Printf("leader %d step down due to heart beat out of date\n", rf.me)
			rf.UpdateTerm(term)
			rf.StepDown()
			rf.mu.Unlock()
			log.Printf("raft %d step down\n", rf.me)
			//time.Sleep(50 * time.Millisecond)
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
	rf.VoteForNewLeaderCh = make(chan bool)
	rf.VotedFor = -1

	rf.minLeaderElecTime = 300
	rf.maxLeaderElecTime = 600

	rf.justGetHeartBeat = false
	rf.justVote = false

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 0
	}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}

	rf.SendEntryCh = make(chan EntryLog)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Run()

	return rf
}
