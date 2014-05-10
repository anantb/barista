package dpaxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "math"
//import "strconv"
import "time"
import "runtime"

//debug print statement function
const debug = false
func pd(arg string){
  if debug{
    log.Printf(arg)
  }
}

//main Paxos Data Structure
type Paxos struct {
  mu sync.Mutex
  plock sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]
  // Your data here.

  //min instance number we can discard up to across all paxos instances
  minSeq int

  //highest instance number we can discard up to on this paxos instance
  highestDone int

  //max instance number seen by this paxos instance
  maxSeq int

  //keeps track of all the highestDone numbers for each Paxos instance 
  //used to find minSeq
  peersMap map[string]int

  //log of outcomes for each paxos instance 
  log map[int]interface{}

  //proposer information for each paxos instance
  proposeinfo map[int]*PaxosProposerInstance

  //acceptor information for each paxos instance
  acceptinfo map[int]*PaxosAcceptorInstance

  epoch int

  leader string

  leaderproposalnum PaxosProposalNum

  tentative map[int]interface{}
}
//***********************************************************************************************************************************//
//Notes
//***********************************************************************************************************************************//
//a lot of functions and crucial data structures are located in common.go


//***********************************************************************************************************************************//
//Paxos Administrative Functions
//***********************************************************************************************************************************//
func(px *Paxos) UpdateEpochLocked(newEpoch int){
  px.mu.Lock()
  defer px.mu.Unlock()
  px.UpdateEpoch(newEpoch)
}
func(px *Paxos) UpdateEpoch(newEpoch int){
  //strictly increasing
  if newEpoch > px.epoch{
     px.epoch = newEpoch
  }
}
func (px *Paxos) SetLeaderAndEpoch(seq int, leader string,newEpoch int){
  px.mu.Lock()
  defer px.mu.Unlock()
  px.leader = leader
  px.leaderproposalnum = px.GetPaxosProposalNum()
  px.UpdateEpoch(newEpoch)
  px.leaderproposalnum.Epoch = px.epoch
}
func (px *Paxos) GetDone() int{
  px.mu.Lock()
  defer px.mu.Unlock()
  return px.highestDone
}
func (px *Paxos) GetMin() int{
  px.mu.Lock()
  defer px.mu.Unlock()

  px.setMin()
  return px.minSeq
}
func (px *Paxos) SetMin(min int) {
  px.mu.Lock()
  defer px.mu.Unlock()
    if min > px.minSeq{
     px.minSeq = min
  }
  for _,peer := range px.peers{
    px.peersMap[peer] = px.minSeq
  }
  px.cleanUp()
}
func (px *Paxos) UpdateMinAndCleanUp(serverName string, maxDone int){
  px.mu.Lock()
  defer px.mu.Unlock()
  px.peersMap[serverName] = maxDone
  //update minSeq and clean up
  px.setMin()
  px.cleanUp()
} 
func (px *Paxos) DeleteFromLog(seq int){
  px.mu.Lock()
  defer px.mu.Unlock()
  delete(px.log,seq)
}
//get's the server name of the current paxos instance
func(px *Paxos) getServerName() string{
  return px.peers[px.me]
}
//returns true if server is the name of the current paxos server,
//false otherwise
func (px *Paxos) isMe(server string) bool{
  if server == px.peers[px.me]{
    return true
  }
  return false
}
func (px *Paxos) GetRPCCount() int{
  return px.rpcCount
}
//generates a new proposal number 
func (px *Paxos) GetPaxosProposalNum() PaxosProposalNum{
  server := px.peers[px.me]

  //generate a hopefully unique proposal number that is larger than all other ones
  //before it
  proposalNum := time.Now().Unix()*int64(len(px.peers)) + int64(px.me)

  //create PaxosProposalNum object
  pn := PaxosProposalNum{px.epoch, server,proposalNum}
  return pn
}
//Returns an integer that represents number of peers required to be a majority
//given the input array of peers
func getMajorityNum(peers []string) int{
  return (len(peers)/2)+1
}

//***********************************************************************************************************************************//
//Proposer Code
//***********************************************************************************************************************************//
func (px *Paxos) FastPropose(seq int, v interface{}, peers []string, failCallback func(int)){
  px.mu.Lock()

  //ensure we have at least one peer
  if len(peers) == 0{
    px.mu.Unlock()
    return
  }

  //check to see if there is already proposer info for this instance (only one dedicated proposer per server)
  //if not then create the required objects
  _,ok := px.proposeinfo[seq]
  if !ok{
    px.proposeinfo[seq] = &PaxosProposerInstance{&PaxosInstanceInfo{seq}, &PaxosProposalNum{px.epoch, px.peers[px.me],0}, *new(sync.Mutex)}
  }

  //update max instance number seen
  if seq > px.maxSeq{
    px.maxSeq = seq
  }

  //get the dedicated proposer info and lock
  proposer := px.proposeinfo[seq] 
  proposal := px.leaderproposalnum;

  px.mu.Unlock()
  
  //instance locking, so that there proposers for different values on the same machine
  //if you did not do this then there could possibly be corruption of internal state, leading to
  //inconsistent results
  proposer.plock.Lock()
  defer proposer.plock.Unlock()

  px.mu.Lock()
  _,okInstance := px.log[seq]
  _,okTentative := px.tentative[seq]
  px.mu.Unlock()

  if okInstance || okTentative{
    return
  }

  //commence proposal cycle, continue until it succeeds
  done := false
  for !done && px.dead == false{
    if proposal.Epoch != px.epoch{
      failCallback(proposal.Epoch)
      return
    }
    //ACCEPT phase
    accepted, explicit_reject, _ := px.ProposerAccept(seq,v,proposal,peers)

    //if the accept phase failed here, this replica may have lost leader status
    if !accepted{ 
      if explicit_reject{
        failCallback(proposal.Epoch)
        return
      }
      time.Sleep(50*time.Millisecond)
      continue
    }
    //LEARN phase
    px.ProposeNotify(seq, v)
    done=true

    px.mu.Lock()
    px.tentative[seq] = v
    px.mu.Unlock()

    break
  }
}
//main proposer function that is called upon start, initiates a dedicated proposer cycle
//seq - instance number
//v - value to proposed if none has been proposed already
//peers - array of peers to use as acceptors
func (px *Paxos) propose(seq int, v interface{}, peers []string){
  px.mu.Lock()

  //ensure we have at least one peer
  if len(peers) == 0{
    px.mu.Unlock()
    return
  }

  _,okInstance := px.log[seq]

  if okInstance{
    px.mu.Unlock()
    return
  }

  //check to see if there is already proposer info for this instance (only one dedicated proposer per server)
  //if not then create the required objects
  _,ok := px.proposeinfo[seq]
  if !ok{
    px.proposeinfo[seq] = &PaxosProposerInstance{&PaxosInstanceInfo{seq}, &PaxosProposalNum{px.epoch, px.peers[px.me],0}, *new(sync.Mutex)}
  }

  //update max instance number seen
  if seq > px.maxSeq{
    px.maxSeq = seq
  }

  //get the dedicated proposer info and lock
  proposer := px.proposeinfo[seq] 

  px.mu.Unlock()
  
  //instance locking, so that there proposers for different values on the same machine
  //if you did not do this then there could possibly be corruption of internal state, leading to
  //inconsistent results
  proposer.plock.Lock()
  defer proposer.plock.Unlock()

  //commence proposal cycle, continue until it succeeds
  done := false
  backOff := 10*time.Millisecond
  for !done && px.dead == false{
    backOff = 10*time.Millisecond
    time.Sleep(time.Duration((rand.Int63() % 100))*time.Millisecond)
    //log.Printf("NormalPropose: before prepare")
    //PREPARE phase
    proposal,value,prepared,error := px.proposerPrepare(seq,v,peers)
    //log.Printf("NormalPropose: after prepare")
    //log.Printf("NormalPropose: after prepare")
    //if the prepare phase failed we cannot proceed, so skip the rest of
    //the loop and try again
    if !prepared{
      //log.Printf("NormalPropose: prepare failed "+px.peers[px.me])
      if !error{
        if backOff<20*time.Millisecond {
          backOff = 2*backOff
          //log.Printf("NormalPropose: prepare backing off")
        }
        //log.Printf("NormalPropose: prepare backing off")
        time.Sleep(backOff)
      }
      continue
    }
    //log.Printf("NormalPropose: before accept")
    //ACCEPT phase
    accepted,_,error := px.ProposerAccept(seq,value,proposal,peers)

    //log.Printf("NormalPropose: after accept")
    //log.Printf("NormalPropose: after accept")
    //if the accept phase failed we cannot proceed, so skip the rest of
    //the loop and try again
    if !accepted{
      //log.Printf("NormalPropose: accept failed")
      if !error{
        if backOff<20*time.Millisecond{
          backOff = 2*backOff
          //log.Printf("NormalPropose: accept backing off")
        }
        //log.Printf("NormalPropose: accept backing off")
        time.Sleep(backOff)
      }
      continue
    }
    //log.Printf("NormalPropose: before notify")
    //backOff = 10*time.Millisecond
    //LEARN phase
    px.ProposeNotify(seq, value)
    done=true
    //log.Printf("NormalPropose: after notify")
    break
  }
  //log.Printf("NormalPropose: prepare succeeded")
}

//function that handles the PREPARE phase of the proposer
//seq - instance number
//v - value to be proposed if none has been proposed already
//peers - array of peers to use as acceptors
//returns the PaxosProposalNum object for which the PREPARE succeeded and the value that ended up being proposed
func (px *Paxos) proposerPrepare(seq int, v interface{}, peers []string) (PaxosProposalNum, interface{},bool,bool){

  //initializations

  //status
  prepared := false
  error := false
  
  //list of servers that send PREPARE_OK along with their replies
  preparedServers := make(map[string]*PrepareReply)

  //number of peers required for a majority
  majority := getMajorityNum(peers)

  //proposal that succeeded
  var proposal PaxosProposalNum

  //value that was selected for proposal and acceptance (based on what we received from peers in this PREPARE phase)
  var value interface{}

  //get new proposal number
  //ensures that no two threads on same machine can get same timestamp
  px.mu.Lock()
  proposalNum := px.GetPaxosProposalNum()
  px.mu.Unlock()
  proposal = proposalNum

  failCount :=  0
  //contact each peer and send do Prepare RPC
  for _,server := range px.peers{

    //prepare arguments and reply
    prepareArgs := &PrepareArgs{seq,proposalNum}
    prepareReply := &PrepareReply{}

    ok := true

    //don't issue RPCs to self
    if px.isMe(server){
      px.Prepare(prepareArgs,prepareReply)
    }else{
      ok = call(server,"Paxos.Prepare",prepareArgs,prepareReply)
    }
    if ok{
      //check status
      switch prepareReply.Status{
        case OK:
          //got PREPARE_OK
          preparedServers[server]=prepareReply
        case REJECT:
          //was rejected, don't count as PREPARE_OK
      }
      px.UpdateEpochLocked(prepareReply.Epoch)
    }else{
      failCount++
    }
    if len(preparedServers) >= majority{
      break
    }
  }

  //if we have a majority of PREPARE_OKs
  if len(preparedServers) >= majority{

    //find the proposal received from peers with the largest
    //proposal number and then replace the value we are proposing with 
    //that value (required for consistency, correctness, and convergence of the Paxos protocol)
    var maxServer *PaxosProposalNum = nil
    for _,prepReply := range preparedServers{

      //if this peer didn't already accept a proposal, it sent back nil
      //so we do not need to count this reply
       if prepReply.MaxProposalAcceptVal == nil{
        continue;
       }

       //if there was at least one non-nil returned value, this means an acceptor already
       //accepted a value and so we need to change our mind to conform to what the acceptor promised
       if maxServer == nil{
        maxServer = &prepReply.MaxProposalAccept
        value = prepReply.MaxProposalAcceptVal
       }else{
        //if we find a value with a bigger proposal number then select it
        if compareProposalNums(&(prepReply.MaxProposalAccept),maxServer) > 0{
          maxServer = &(prepReply.MaxProposalAccept)
          value = prepReply.MaxProposalAcceptVal
        }
      }
    }
    //if all the replies from peers contained nil accepted values then
    //this paxos instance is free to propose its own value
    if maxServer == nil{
      maxServer = &proposalNum
      value = v
    }

    //update status, prepare succeeded
    prepared = true
  }
  if failCount > 0{
    error = true
  }
  return proposal,value,prepared, error
}
//function that handles the ACCEPT phase of the proposer
//seq - instance number
//v - value that peers should accept
//proposal - PaxosProposalNum object that represents the proposal number to use in acceptance (the proposal acceptor peers said they would accept)
//peers - array of peers to use as acceptors
//returns true if a majority of the peers accepted (ACCEPT Phase succeeded)
func (px *Paxos) ProposerAccept(seq int, v interface{}, proposal PaxosProposalNum, peers []string) (bool,bool,bool){
  //initializations

  //status
  accepted := false
  error := false
  explicit_reject := false

  //list of servers that send ACCEPT_OK along with their replies
  acceptedServers := make(map[string]*AcceptReply)

  //number of peers required for a majority
  majority := getMajorityNum(peers)

  failCount := 0

  //contact each peer and send do Accept RPC
  for _,server := range px.peers{

    //accept args and reply
    acceptArgs := &AcceptArgs{seq,proposal,v}
    acceptReply := &AcceptReply{}

    ok := true

    //don't issue Accept RPC to self, just call the function
    if px.isMe(server){
      px.Accept(acceptArgs,acceptReply)
    }else{
      ok = call(server,"Paxos.Accept",acceptArgs,acceptReply)
    }
    if ok{
      //check status
      switch acceptReply.Status{
        case OK:
          //got ACCEPT_OK
          acceptedServers[server]=acceptReply
        case REJECT:
          //was rejected, don't count as ACCEPT_OK
          explicit_reject = true
      }
      px.UpdateEpochLocked(acceptReply.Epoch)
    }else{
      failCount++
    }
    if len(acceptedServers) >= majority{
      accepted = true
      break
    }
  }
  //if we got a majority of ACCEPT_OKs, the ACCEPT phase succeded
  if failCount > 0{
    error = true
  }
  return accepted,explicit_reject,error
}
func (px *Paxos) ProposeNotify(seq int, value interface{}){
   //notify everyone of agreement
    //TODO: put this in separate method
    teachArgs := &TeachArgs{}
    teachReply := &TeachReply{}

    //map, just in case you want to send multipl results
    out := make(map[int]interface{})
    out[seq]=value
    teachArgs.Log = out

    //tell about max done
    teachArgs.MaxDone = px.highestDone

    //tell peer who is talking to it
    teachArgs.ServerName = px.getServerName()

    //notify all of decision
    for _,server := range px.peers{
      if px.isMe(server){
        px.Teach(teachArgs,teachReply)
      }else{
        call(server,"Paxos.Teach",teachArgs,teachReply)
      }
    }
}
//***********************************************************************************************************************************//
//Acceptor Code
//***********************************************************************************************************************************//

//PREPARE RPC handler for acceptor
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  //get instance and proposal numbers
  instancenum := args.InstanceNum
  proposalnum := &args.ProposalNum

  //check to see if acceptor objects have been initialized for this instance,
  //if not then create them
  _,ok := px.acceptinfo[instancenum]
  if !ok{
    paxosInstanceInfo := &PaxosInstanceInfo{InstanceNum: instancenum}
    acceptor := &PaxosAcceptorInstance{pi:paxosInstanceInfo}
    px.acceptinfo[instancenum] = acceptor
  }

  //update max instance number seen by this paxos machine
  if instancenum > px.maxSeq{
    px.maxSeq = instancenum
  }

  //get the acceptor object (state) for this instance
  acceptor,_ := px.acceptinfo[instancenum]

  px.UpdateEpoch(proposalnum.Epoch)
  reply.Epoch = px.epoch
  if proposalnum.Epoch < px.epoch{
    reply.Status = REJECT

    //TODO: maybe do something here
    return nil
  }
  //if the acceptor has not received a proposal yet then accept the first proposal
  //otherwise respond to proposal based on if current proposal num > max proposal num seen
  if acceptor.MaxProposal != nil{

    //if the current proposal number > max proposal num seen
    if compareProposalNums(proposalnum, acceptor.MaxProposal) > 0{

      //update max proposal num seen
      acceptor.MaxProposal = proposalnum

      //if this acceptor previous accepted a value then inform the 
      //proposer in the PREPARE phase so that the proposer can change 
      //its mind and proposer the right (this) value  
      if acceptor.MaxAcceptedProposalNum != nil{
        reply.MaxProposalAccept = *acceptor.MaxAcceptedProposalNum
        reply.MaxProposalAcceptVal = acceptor.MaxAcceptedProposalVal
      }

      //promise to reject any proposals with smaller proposal numbers
      reply.Status = OK
    }else{
      //if we got a smaller proposal number than max proposal num seen
      //reject the PREPARE request but tell the proposer about the max proposal num seen
      reply.MaxProposal = *acceptor.MaxProposal
      reply.Status = REJECT
      //log.Println("proposal rejected proposal number was too small!")
    }
  }else{
    //if the acceptor has not received a proposal yet then accept the first proposal
    acceptor.MaxProposal = proposalnum
    reply.Status = OK
  }
  return nil
}
//ACCEPT RPC handler for the acceptor
func (px *Paxos) Accept(args *AcceptArgs,reply *AcceptReply) error{
  px.mu.Lock()
  defer px.mu.Unlock()

  //get instance and proposal numbers
  instancenum := args.InstanceNum
  proposalnum := &args.ProposalNum

  //get the acceptor object/state for this instance

  _,ok := px.acceptinfo[instancenum]
  if !ok{
    paxosInstanceInfo := &PaxosInstanceInfo{InstanceNum: instancenum}
    acceptor := &PaxosAcceptorInstance{pi:paxosInstanceInfo}
    px.acceptinfo[instancenum] = acceptor
  }
  acceptor,_ := px.acceptinfo[instancenum]
  //update max instance number seen by this paxos machine
  if instancenum > px.maxSeq{
    px.maxSeq = instancenum
  }
  px.UpdateEpoch(proposalnum.Epoch)
  reply.Epoch = px.epoch
  if proposalnum.Epoch < px.epoch{
    reply.Status = REJECT

    //TODO: maybe do something here
    return nil
  }
  //fmt.Printf(px.peers[px.me]+" seq = %v current epoch = %v \n",instancenum,px.epoch)
  //if the current proposal >= max proposal number seem
  //then accept the proposal, otherwise we got an old
  //proposal so reject it
  if compareProposalNums(proposalnum, acceptor.MaxProposal) >= 0{
    /*if(acceptor.MaxProposal != nil){
      fmt.Printf(px.peers[px.me]+" seq = %v current proposal epoch = %v, current max epoch = %v \n",instancenum, proposalnum.Epoch, acceptor.MaxProposal.Epoch)
    }*/
    //update acceptor state

    acceptor.MaxProposal = proposalnum
    acceptor.MaxAcceptedProposalNum = proposalnum
    acceptor.MaxAcceptedProposalVal = args.ProposalVal

    //send ACCEPT_OK
    reply.Status = OK
  }else{
    //reject the accept
    reply.MaxProposal = *acceptor.MaxProposal
    reply.Status = REJECT
  }

  return nil
}
//***********************************************************************************************************************************//
//Learner Code
//***********************************************************************************************************************************//
//LEARN (teach) RPC, called by proposers after ACCEPT phase has completed
//so that peers can learn about the decision 
func (px *Paxos) Teach(args *TeachArgs, reply *TeachReply) error{
  px.mu.Lock()
  defer px.mu.Unlock()

  //don't block, let the RPC return so that it doesn't timeout
  go px.handleTeach(args.ServerName,args.MaxDone,args.Log)

  return nil
}
//handles updating the paxos log with what happened for that instance it learned about,
//also handles updating minSeq (so that Min() is accurate), and free space based on minSeq (Min())
func (px *Paxos) handleTeach(serverName string, maxDone int, log map[int]interface{}){
  px.mu.Lock()
  defer px.mu.Unlock()
  //update log
  for key,val := range log{
    px.log[key] = val
  }
  px.peersMap[serverName] = maxDone

  //update minSeq and clean up
  px.setMin()
  px.cleanUp()

}
//***********************************************************************************************************************************//
//More Paxos Administrative Functions
//***********************************************************************************************************************************//

//figure ous what the Min (minSeq) value should be
//for the Min() function
func (px *Paxos) setMin(){

  //start at max possible value
  min := math.MaxInt32

  //scan over all peers and find the min of all maxDone
  //values they have sent us
  for _,key := range px.peers{
    val,_:=px.peersMap[key]
    if val < min{
      min = val
    }
  }
  //if a peer hasn't sent us a maxDone value
  //then min will be -1 for this peer and we
  //cannot yet set minSeq because a peer may still
  //need this machine's data
  if min != -1 && min>=px.minSeq{
    px.minSeq = min
  }
}
//frees space based on the minSeq( Min() value)
func (px *Paxos) cleanUp(){
  seq := px.minSeq

  //free log entries
  for lkey,_ := range px.log{
    if lkey < seq{
      delete(px.log,lkey)
    }
  }

  //free tentative info
  for tkey,_ := range px.tentative{
    if tkey < seq{
      delete(px.tentative,tkey)
    }
  }

  //free proposer info
  for pkey,_ := range px.proposeinfo{
    if pkey < seq{
      delete(px.proposeinfo,pkey)
    }
  }

  //free acceptor info
  for akey,_ := range px.acceptinfo{
    if akey < seq{
      delete(px.acceptinfo,akey)
    }
  }

  runtime.GC()
}
func (px *Paxos) CleanAfter(seq int){
  //free log entries
  for lkey,_ := range px.log{
    if lkey > seq{
      delete(px.log,lkey)
    }
  }
  //free tentative info
  for tkey,_ := range px.tentative{
    if tkey > seq{
      delete(px.tentative,tkey)
    }
  }
  runtime.GC()
}
//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()
  if seq >= px.minSeq{
    go px.propose(seq,v,px.peers)
  }
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.mu.Lock()
  defer px.mu.Unlock()
  // Your code here.
  if seq > px.highestDone{
    px.highestDone = seq
    px.peersMap[px.peers[px.me]] = px.highestDone 
    //update minSeq and clean up
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  // Your code here.
  return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  // You code here.
  return px.minSeq+1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()

  if seq > px.maxSeq{
    px.maxSeq = seq
  }

  val,ok := px.log[seq]
  if ok && seq >= px.minSeq{
    return ok,val
  }else{
    return false, nil
  }
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}
//
func (px *Paxos) SetUnreliable(unreliable bool) {
  px.unreliable = unreliable
}
//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.mu = sync.Mutex{}

  // Your initialization code here.
  px.maxSeq = 0
  px.minSeq = -1
  px.log = make(map[int]interface{})
  px.tentative = make(map[int]interface{})
  px.proposeinfo = make(map[int]*PaxosProposerInstance)
  px.acceptinfo = make(map[int]*PaxosAcceptorInstance)
  px.peersMap = make(map[string]int)
  px.epoch = 0
  px.leader = ""
  px.leaderproposalnum = px.GetPaxosProposalNum()

  //init peers map with maxDone values,
  //-1 represents we haven't heard from that peer
  for _,val := range px.peers{
    px.peersMap[val]=-1
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }
  return px
}
