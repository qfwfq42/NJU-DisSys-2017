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
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const HeartbeatInterval = 50
const minElectionTimeout = 150
const maxElectionTimeout = 300 //according to the paper
const (
	roleFollower = iota
	roleCandidate
	roleLeader
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	CurrentTerm int //latest term server has seen
	VotedFor    int
	Log         []LogEntry
	State       int
	Timer       *time.Timer
	//Volatile state on all servers
	CommitIndex int
	LastApplied int
	//Volatile state on leaders.Reinitialized after election.
	NextIndex  map[int]int
	MatchIndex map[int]int
	ApplyCh    chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CommitChan chan struct{}
}

func Min(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock() //use Mutex until return
	term = rf.CurrentTerm
	//fmt.Printf("[termCheck]:%d", term)
	if rf.State == roleLeader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.Log)
	e.Encode(rf.VotedFor)
	e.Encode(rf.CurrentTerm)
	//e.Encode(rf.CommitIndex)
	//e.Encode(rf.LastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.Log)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.CurrentTerm)
	//d.Decode(&rf.CommitIndex)
	//d.Decode(&rf.LastApplied)
}

//AppendEntriesArgs RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int //leader’s term
	LeaderId     int
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store
	LeaderCommit int        //leader’s commitIndex
}

//AppendEntriesReply RPC arguments structure.
type AppendEntriesReply struct {
	Term    int //currentTerm, for leader to update itself
	Success bool
}

//AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock() //use Mutex until return
	//fmt.Printf("[rf]%+v\n", *rf)
	if args.Term > rf.CurrentTerm {
		rf.TransToFollower(args.Term)
	}
	if args.Term == rf.CurrentTerm {
		if rf.State != roleFollower {
			rf.TransToFollower(args.Term)
		}
		rf.ResetElectionTimer()
		if args.PrevLogIndex == -1 || (args.PrevLogIndex < len(rf.Log) && args.PrevLogTerm == rf.Log[args.PrevLogIndex].Term) {
			reply.Success = true
			insertIndex := args.PrevLogIndex + 1
			newEntryIndex := 0
			/*
				for (insertIndex >= len(rf.Log) || newEntryIndex >= len(args.Entries)) && rf.Log[insertIndex].Term != args.Entries[newEntryIndex].Term {
					insertIndex += 1
					newEntryIndex += 1
				}
			*/
			for {
				if insertIndex >= len(rf.Log) || newEntryIndex >= len(args.Entries) {
					break
				}
				if rf.Log[insertIndex].Term != args.Entries[newEntryIndex].Term {
					break
				}
				newEntryIndex += 1
				insertIndex += 1
			}
			if newEntryIndex < len(args.Entries) {
				rf.Log = append(rf.Log[:insertIndex], args.Entries[newEntryIndex:]...)
			}
			if args.LeaderCommit > rf.CommitIndex {
				rf.CommitIndex = Min(args.LeaderCommit, len(rf.Log)-1)
				rf.CommitChan <- struct{}{}
			}
		}
	} else {
		reply.Success = false
	}
	reply.Term = rf.CurrentTerm
	rf.persist()
}

func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) BroadcastAppendEntries() {
	rf.mu.Lock()
	bufferCurrentTerm := rf.CurrentTerm
	peerCount := len(rf.peers)
	rf.mu.Unlock()
	//fmt.Printf("Broadcast:%d nowTime:%v\n", rf.me, time.Now().UnixNano()/1e6)
	for i := 0; i < peerCount; i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			nextIndex_i := rf.NextIndex[i]
			entries := rf.Log[nextIndex_i:]
			prevLogIndex := nextIndex_i - 1
			var prevLogTerm int
			if prevLogIndex < 0 {
				prevLogTerm = -1
			} else {
				prevLogTerm = rf.Log[prevLogIndex].Term
			}

			args := new(AppendEntriesArgs)
			args.Term = bufferCurrentTerm
			args.LeaderId = rf.me
			args.PrevLogTerm = prevLogTerm
			args.PrevLogIndex = prevLogIndex
			args.LeaderCommit = rf.CommitIndex
			args.Entries = entries
			//fmt.Printf("[args]%+v\n", *args)
			reply := new(AppendEntriesReply)
			rf.SendAppendEntries(i, *args, reply)
			//fmt.Printf("replyTerm:%d\n", reply.Term)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if bufferCurrentTerm < reply.Term {
				rf.TransToFollower(reply.Term)
				return
			}

			if rf.State == roleLeader && bufferCurrentTerm == reply.Term {
				if reply.Success {
					bufferCommitIndex := rf.CommitIndex
					rf.NextIndex[i] = nextIndex_i + len(entries)
					rf.MatchIndex[i] = rf.NextIndex[i] - 1
					for i := bufferCommitIndex + 1; i < len(rf.Log); i++ {
						if rf.Log[i].Term == rf.CurrentTerm {
							Count := 1
							for j := 0; j < len(rf.peers); j++ {
								if j == rf.me {
									continue
								}
								if rf.MatchIndex[j] >= i {
									Count += 1
								}
							}
							if Count > len(rf.peers)/2 {
								rf.CommitIndex = i
							}
						}

					}
					if rf.CommitIndex != bufferCommitIndex {
						rf.CommitChan <- struct{}{}
					}
				} else {
					rf.NextIndex[i] = nextIndex_i - 1
				}
			}
		}(i)
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock() //use Mutex until return
	bufferLastLogIndex := len(rf.Log) - 1
	var bufferLastLogTerm int
	if bufferLastLogIndex >= 0 {
		bufferLastLogTerm = rf.Log[bufferLastLogIndex].Term
	} else {
		bufferLastLogTerm = -1
	}
	//fmt.Printf("thisID:%d targetsID:%d targetTerm:%d\n", args.CandidateId, rf.me, rf.CurrentTerm)
	if args.Term > rf.CurrentTerm {
		//fmt.Printf("[x]%d %d \n", args.CandidateId, rf.VotedFor)
		//fmt.Printf("2thisID:%d targetsID:%d targetTerm:%d\n", args.CandidateId, rf.me, rf.CurrentTerm)
		rf.TransToFollower(args.Term)
	}
	//fmt.Printf("[request] %d, %d \n", rf.CurrentTerm, rf.VotedFor)
	if args.Term == rf.CurrentTerm && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) &&
		(args.LastLogTerm > bufferLastLogTerm || (args.LastLogTerm == bufferLastLogTerm && args.LastLogIndex >= bufferLastLogIndex)) {
		//fmt.Printf("[y]%d %d \n", args.CandidateId, rf.VotedFor)
		rf.ResetElectionTimer()
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
	} else {
		//fmt.Printf("[z]%d %d \n", args.CandidateId, rf.VotedFor)
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	}
	rf.persist()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) SendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	bufferCurrentTerm := rf.CurrentTerm
	bufferLastLogIndex := len(rf.Log) - 1
	bufferLastLogTerm := rf.Log[bufferLastLogIndex].Term
	peerCount := len(rf.peers)
	//fmt.Printf("[start2]t:%d p:%d\n", bufferCurrentTerm, peerCount)
	rf.mu.Unlock()
	nowVotes := 1

	replyArray := make([]RequestVoteReply, peerCount)
	for i := 0; i < peerCount; i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {

			args := new(RequestVoteArgs)
			args.Term = bufferCurrentTerm
			args.CandidateId = rf.me
			args.LastLogTerm = bufferLastLogTerm
			args.LastLogIndex = bufferLastLogIndex
			//go rf.SendRequestVote(i, *args, &replyArray[i])
			if ok := rf.SendRequestVote(i, *args, &replyArray[i]); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.State != roleCandidate {
					return
				}
				if replyArray[i].Term > bufferCurrentTerm {
					rf.TransToFollower(replyArray[i].Term)
					return
				}
				if replyArray[i].VoteGranted && replyArray[i].Term == rf.CurrentTerm {
					nowVotes += 1
					//fmt.Printf("nowVotes %d\n", nowVotes)
					if nowVotes == peerCount/2+1 && rf.State == roleCandidate {
						go rf.TransToLeader() //win the vote, become leader
						return
					}
				}
			}
		}(i)
	}

	go rf.ElectionTimer()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State == roleLeader {
		//index = rf.NextIndex[rf.me]
		term = rf.CurrentTerm
		index = len(rf.Log)
		//fmt.Printf("\nNOWINDEX: %d\n\n", index)
		rf.Log = append(rf.Log, LogEntry{term, command})
		rf.persist()
		//rf.BroadcastAppendEntries()
	} else {
		isLeader = false
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) FollowerAction() {
	// Your code here, if desired.
}

func (rf *Raft) CandidateAction() {
	// Your code here, if desired.
}

func (rf *Raft) ResetElectionTimer() {
	bufferTime := minElectionTimeout + rand.Intn(maxElectionTimeout-minElectionTimeout)
	//fmt.Printf("ID:%d, time:%d \n", rf.me, bufferTime)
	rf.Timer.Reset(time.Duration(bufferTime) * time.Millisecond)
}

func (rf *Raft) ElectionTimer() {
	bufferTerm := rf.CurrentTerm

	for {
		select {
		case <-rf.Timer.C:
			rf.mu.Lock()
			if rf.State == roleLeader || bufferTerm != rf.CurrentTerm {
				rf.mu.Unlock()
				return
			}
			//fmt.Printf("[timer]no:%d\n", rf.me)
			rf.mu.Unlock()
			rf.TransToCandidate()
			//fmt.Printf("[timer2]no:%d\n", rf.me)
			return
		}
	}
}

func (rf *Raft) TransToFollower(term int) {
	rf.State = roleFollower
	rf.CurrentTerm = term
	rf.VotedFor = -1
	bufferTime := minElectionTimeout + rand.Intn(maxElectionTimeout-minElectionTimeout)
	//fmt.Printf("[bufferTime]no:%d, time:%d\n", rf.me, bufferTime)
	rf.Timer = time.NewTimer(200 * time.Millisecond)
	rf.Timer.Reset(time.Duration(bufferTime) * time.Millisecond)
	go rf.ElectionTimer()
}

func (rf *Raft) TransToCandidate() {

	rf.State = roleCandidate
	rf.CurrentTerm += 1     //Increment currentTerm
	rf.VotedFor = rf.me     //Vote for self
	rf.ResetElectionTimer() //Reset election timer
	//Printf("[toCandidate]no:%d, term:%d NOWTIME:%v\n", rf.me, rf.CurrentTerm, time.Now().UnixNano()/1e6)
	rf.StartElection() //Send RequestVote RPCs to all other servers
}

func (rf *Raft) TransToLeader() {
	//fmt.Printf("xxx\n")
	if rf.State == roleLeader {
		return
	}
	//fmt.Printf("toLeader No:%d term:%d NOWTIME:%v\n", rf.me, rf.CurrentTerm, time.Now().UnixNano()/1e6)
	rf.State = roleLeader
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.NextIndex[i] = len(rf.Log)
		rf.MatchIndex[i] = 0
	}
	//fmt.Printf("[nextIndex]:%v\n", rf.NextIndex)
	go func() {
		//)
		for {
			if rf.State == roleLeader {
				rf.BroadcastAppendEntries()
				//fmt.Printf("[LeaderState]no:%d term:%d NOWTIME:%v\n", rf.me, rf.CurrentTerm, time.Now().UnixNano()/1e6)
				time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
				//fmt.Printf("aftersleep %d NOWTIME:%v\n", rf.State, time.Now().UnixNano()/1e6)
			}
		}
	}()
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
	rf.ApplyCh = applyCh
	// Your initialization code here.
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.Log = append(rf.Log, LogEntry{0, 0})
	rf.CommitChan = make(chan struct{}, 32)
	rf.NextIndex = make(map[int]int)
	rf.MatchIndex = make(map[int]int)
	//initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//fmt.Printf("[Make]no:%d\n", rf.me)
	go rf.TransToFollower(rf.CurrentTerm)

	go rf.ApplyShSender()
	return rf
}

func (rf *Raft) ApplyShSender() {
	for range rf.CommitChan {
		//fmt.Printf("[applyShSender]%+v\n", *rf)
		bufferLastApplied := rf.LastApplied
		var entries []LogEntry //to be applied
		rf.persist()
		//if commitIndex > lastApplied,then incremente lA & apply log[lA]
		if rf.CommitIndex > rf.LastApplied {
			entries = rf.Log[rf.LastApplied+1 : rf.CommitIndex+1]
			rf.LastApplied = rf.CommitIndex
		}
		//apply to state machine
		for i, entry := range entries {
			rf.ApplyCh <- ApplyMsg{
				Command: entry.Command,
				Index:   bufferLastApplied + i + 1,
			}
		}
		//fmt.Printf("[applyShSender2]%+v\n", *rf)
	}
}
