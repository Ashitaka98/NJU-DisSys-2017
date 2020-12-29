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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
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
	LogIndex int
	LogTerm int
	LogCmd interface{}
}

type Status int
const(
	follower Status = 0
	candidate Status = 1
	leader Status = 2
)

const HeartBeatInterval = 100*time.Millisecond
const ElectionTimeOut = 1000*time.Millisecond

const debug bool=false

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int	// term of this raft, term will increase only when an election timeout encountered
					// or a message of newer term received (maybe a vote request or a heartbeat)
	votedFor	int	// a follower in each term only vote once,
					// voteFor is the candidateId received vote in current term, -1 if none
	logs []LogEntry	// log entries; each entry contains command
					// for state machine, and term when entry
					// was received by leader (first index is 1)

	status Status	// follower candidate or leader

	// for each raft server to manage log and state machine
	commitIndex int	// index of highest log entry known to be
					// committed (initialized to 0, increases monotonically)
	lastApplied int	// index of highest log entry applied to state
					// machine (initialized to 0, increases monotonically)


	// for leader to sync follower
	nextIndex []int	// for each server, index of the next log entry
					// to send to that server (initialized to leader last log index + 1)

	matchIndex []int	// for each server, index of highest log entry
						// known to be replicated on server (initialized to 0, increases monotonically)


	// channel
	applyChn chan ApplyMsg	// channel for raft to receive client's apply message
	statusChange chan bool	// channel to send raft main thread a status change signal

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.status == leader{
		isLeader = true
	} else {
		isLeader = false
	}
	return term, isLeader
}

func (rf *Raft) getLastLogIndex() int{	// at least one empty entry will be in logs
	return rf.logs[len(rf.logs)-1].LogIndex
}

func (rf *Raft) getLastLogTerm() int{	// at least one empty entry will be in logs
	return rf.logs[len(rf.logs)-1].LogTerm
}



//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool

}

// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term int	// leader's term
	LeaderId int	// so followers can redirect clients
	PrevLogIndex int	// index of log entry immediately preceding new ones
	PrevLogTerm int		// term of prevLogIndex entry
	LeaderCommit int	// leader's commitIndex
	Entries []LogEntry	// log entries to store(empty for heartbeat; may send more than one at a time)
}

type AppendEntriesReply struct {
	Term int	// for leader to update itself
	Success bool	//true if follower contained entry matching prevLogIndex and prevLogTerm
	NextIndex int
}

//
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	var statusChangeSignal bool = false
	rf.mu.Lock()	// protect rf.currentTerm, rf.status adn rf.logs
	defer rf.mu.Unlock()
	if debug {
		fmt.Printf("I'm %d, status is %d, receiving vote request from %d whose term is %d and my term is %d \n", rf.me, rf.status, args.CandidateId, args.Term, rf.currentTerm)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {	// a vote with out-of-date term will be rejected directly
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {	// a vote with newer term will update this server
		rf.status = follower
		rf.votedFor=-1
		rf.currentTerm = args.Term
		rf.statusChange<-true
		statusChangeSignal = true
	}
	if args.LastLogIndex < rf.getLastLogIndex() {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		if !statusChangeSignal {
			rf.statusChange <- true
		}
		return
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply){

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if debug {
		fmt.Printf("I'm %d, status is %d, receiving heartbeat from %d whose term is %d and my term is %d \n", rf.me, rf.status, args.LeaderId, args.Term, rf.currentTerm)
	}
	reply.Term = rf.currentTerm
	if args.Term<rf.currentTerm{
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm{
		reply.Success = true
		rf.status = follower
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		rf.statusChange<-true
	}else {
		reply.Success = true
		rf.status = follower
		rf.votedFor = args.LeaderId
		rf.statusChange<-true
	}
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries",args,reply)
	return ok
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	index:=-1
	term:=rf.currentTerm
	isLeader:=false
	if rf.status==leader{
		isLeader = true
		index=rf.getLastLogIndex()+1
		rf.logs = append(rf.logs,LogEntry{LogTerm: term,LogIndex: index,LogCmd: command})
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


func (rf *Raft) broadcastRequestVote(args RequestVoteArgs) {	// send vote request to each peer, is invoked by main thread

	if debug {
		fmt.Printf("I'm %d broadcasting RequestVote\n", rf.me)
	}

	var voteCount int = 1	// voteCount counts the votes it got and will be reset everytime a vote is initialized
	n:=len(rf.peers)
	for i :=range rf.peers{	// create thread for each vote request
		if i==rf.me{	// skip itself
			continue
		}
		go func(sendTo int) { // thread sending vote request and handling reply

			var reply RequestVoteReply
			ok := rf.sendRequestVote(sendTo,args,&reply) // should be blocked before call returned
			if debug {
				fmt.Printf("I'm %d receiving vote reply from %d  result: %t \n", rf.me, sendTo, reply.VoteGranted)
			}
			rf.mu.Lock()	// use this lock to protect rf.voteCount and replyCount
			defer rf.mu.Unlock()
			if !ok {	// rpc was not successfully delivered, give up this request
				return
			}
			if reply.VoteGranted{
				voteCount++
				if rf.status==candidate && rf.currentTerm==args.Term && 2*voteCount>n{	// elected to be leader, maybe
					rf.status = leader
					rf.votedFor = rf.me
					rf.currentTerm++
					// reinitialize rf.nextIndex and rf.matchIndex after an election
					for i:=range rf.nextIndex{
						rf.nextIndex[i] = rf.getLastLogIndex()+1
						rf.matchIndex[i] = 0
					}
					// send an empty heartbeat
					var args AppendEntriesArgs
					args.LeaderId = rf.me
					args.Term = rf.currentTerm
					args.PrevLogTerm = rf.getLastLogTerm()
					args.PrevLogIndex = rf.getLastLogIndex()
					args.LeaderCommit = rf.commitIndex
					args.Entries = make([]LogEntry,0)
					rf.broadcastAppendEntries(args)
					rf.statusChange<-true
				}
			} else{
				if reply.Term>rf.currentTerm{	// my term is out of date
					rf.status = follower
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.statusChange<-true
				}
			}
		}(i)
	}
}

func (rf *Raft)broadcastAppendEntries(args AppendEntriesArgs){

	if debug {
		fmt.Printf("I'm %d broadcasting heartbeat \n", rf.me)
	}
	for i := range rf.peers{
		if i == rf.me{
			continue
		}
		go func(sendTo int) {
			if debug {
				fmt.Printf("I'm %d sending heartbeat to %d args term: %d \n", rf.me, sendTo, args.Term)
			}
			var reply AppendEntriesReply

			ok:=rf.sendAppendEntries(sendTo,args,&reply)

			if !ok{
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term>rf.currentTerm{
				rf.status = follower
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.statusChange<-true
			}
		}(i)
	}
}

// this function should be invoked in critical zone and will generate signal for main thread(important).
// there are four cases(driven by rpc handler or broadcast, not by timer):
// follower to follower(a newer msg or heartbeat received, invoked by handler),
// candidate to leader(win the vote, invoked by broadcast),
// candidate to follower(a newer msg received, invoked by handler or broadcast),
// leader to follower(a newer msg received, invoked by handler or broadcast)


func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	n:=len(rf.peers)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{LogIndex: 0,LogTerm: 0})	// put an empty log at first

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, n)
	rf.nextIndex = make([]int, n)

	rf.status = follower
	rf.applyChn = applyCh
	rf.statusChange = make(chan bool, 10)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//raft main thread, will race with rpc handler
	go func() {
		// timer
		electionTimer:=time.NewTimer(ElectionTimeOut)
		heartBeatTimer:=time.NewTimer(HeartBeatInterval)
		heartBeatTimer.Stop()
		if debug {
			fmt.Printf("I'm %d initializing\n", rf.me)
		}
		for{
			// event driven programing, all timer related operation done here
			switch rf.status {
			case follower:
				if debug {
					fmt.Printf("I'm %d follower\n", rf.me)
				}
				select {
				// follower to follower
				case <-rf.statusChange:
					if debug {
						fmt.Printf("I'm %d follower reset timer\n", rf.me)
					}
					electionTimer.Stop()
					electionTimer.Reset(ElectionTimeOut)
				// follower to candidate
				case <-electionTimer.C:	// an election timer out, follower become candidate
					rf.mu.Lock()
					rf.status = candidate
					rf.votedFor = rf.me
					rf.currentTerm++
					args := RequestVoteArgs{rf.currentTerm,rf.me,rf.getLastLogIndex(),rf.getLastLogTerm()}
					rf.mu.Unlock()
					electionTimer.Stop()
					electionTimer.Reset(ElectionTimeOut)
					go rf.broadcastRequestVote(args)
				}
				break
			case candidate:
				if debug {
					fmt.Printf("I'm %d candidate\n", rf.me)
				}
				select {
				case <-rf.statusChange:	// change to follower or leader
					rf.mu.Lock()
					if rf.status == leader{
						electionTimer.Stop()
						heartBeatTimer.Stop()
						heartBeatTimer.Reset(HeartBeatInterval)
					}else if rf.status == follower{
						electionTimer.Stop()
						electionTimer.Reset(ElectionTimeOut)
					}
					rf.mu.Unlock()
				case <-electionTimer.C:	// an unsuccessful vote, try again
					rf.mu.Lock()
					rf.currentTerm++
					rf.votedFor = rf.me
					args := RequestVoteArgs{rf.currentTerm,rf.me,rf.getLastLogIndex(),rf.getLastLogTerm()}
					rf.mu.Unlock()
					delta:=time.Duration(rand.Int31()%1000)*time.Millisecond
					electionTimer.Stop()
					electionTimer.Reset(ElectionTimeOut+delta)
					go rf.broadcastRequestVote(args)
				}
				break
			case leader:
				if debug {
					fmt.Printf("I'm %d leader \n", rf.me)
				}
				select {
				case <-rf.statusChange:	// leader to follower
					heartBeatTimer.Stop()
					electionTimer.Stop()
					electionTimer.Reset(ElectionTimeOut)
				case <-heartBeatTimer.C:
					rf.mu.Lock()

					var args AppendEntriesArgs
					args.LeaderId = rf.me
					args.Term= rf.currentTerm
					args.PrevLogTerm = rf.getLastLogTerm()
					args.PrevLogIndex = rf.getLastLogIndex()
					args.LeaderCommit = rf.commitIndex

					rf.mu.Unlock()
					rf.broadcastAppendEntries(args)
					heartBeatTimer.Stop()
					heartBeatTimer.Reset(HeartBeatInterval)
				}
				break
			}
		}
	}()
	return rf
}
