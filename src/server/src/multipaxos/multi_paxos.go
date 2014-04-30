package multipaxos

import "dpaxos"
import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
//import "math"
//import "strconv"
import "time"
//import "runtime"

type MultiPaxos struct{
	mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  px *dpaxos.Paxos
  me int
  peers []string
  leader MultiPaxosLeader
  executionPointer int
  transition bool
  results map[int]*MultiPaxosOP
}
func (mpx *MultiPaxos) isInit() bool{
  return mpx.leader.epoch > 0
}
func (mpx *MultiPaxos) isLeader() bool{
  l := mpx.leader
  if(mpx.isInit()){
    if(l.id==mpx.me){
      return true
    }
  }
  return false
}
func (mpx *MultiPaxos) generateLeaderOpProposal() PaxosProposalNum{
    ppn := PaxosProposalNum{}
    ppn.Epoch = mpx.leader.epoch
    ppn.ServerName = mpx.peers[mpx.me]
    ppn.ProposalNum = 1
    return ppn
}
func (mpx *MultiPaxos) LeaderStart(seq int, v interface{}){
    ppn := mpx.generateLeaderOpProposal()
    accepted,error := px.ProposerAccept(seq,v,ppn,mpx.peers)
    px.ProposeNotify(seq, v)
}
func (mpx *MultiPaxos) Start(seq int, v interface{}) (bool,string,string){
  if(seq < 1){
    return false, INVALID_INSTANCE, ""
  }
  if mpx.isLeader(){
    mop := MultiPaxosOP{}
    mop.Type = NORMAL
    mop.EpochNum = mpx.leader.epoch
    mop.Op = v
    
    //Old Start: mpx.px.Start(seq,mop)
    //New Start:
    go mpx.LeaderStart(seq,v)
    return true,OK, ""
  }
  if(mpx.isInit()){
    return false,WRONG_SERVER, mpx.peers[mpx.leader.id]
  }else{
    for !mpx.isInit(){
      //loop spin, maybe do something better here
    }
  }
}


func (mpx *MultiPaxos) Done(seq int) {
  mpx.px.Done(seq)
}


func (mpx *MultiPaxos) Max() int {
  return mpx.px.Max()
}


func (mpx *MultiPaxos) Min() int {
  return mpx.px.Min()
}

func (mpx *MultiPaxos) Status(seq int) (bool, interface{}) {
  ok,val := mpx.results[seq]
  return ok,val
}
func (mpx *MultiPaxos) SetUnreliable(unreliable bool){
  mpx.unreliable = unreliable
  mpx.px.SetUnreliable(unreliable)
}
func (mpx *MultiPaxos) Kill() {
  mpx.dead = true
  if mpx.l != nil {
    mpx.l.Close()
  }
  mpx.px.Kill()
}
func (mpx *MultiPaxos) commitAndLogInstance(executionPointer int, val interface{}){
  mpx.mu.Lock()
  defer func(){
                //represents the commit point for the application of a 
                //command agreed upon for a particular slot in the Paxos log

                //calls done for instance earlier than the one just processed
                mpx.Done(mpx.executionPointer)

                //increments execution pointer so that we can start waiting for
                //the next entry of the Paxos log to be agreed upon and so can process it
                mpx.executionPointer+=1


                mpx.mu.Unlock()
              }()
  //at most once RPC
  mop := val.(MultiPaxosOP)
  switch(mop.Type){
    case NORMAL:
      //do nothing
    case LCHANGE:
      mplc := mop.Op.(MultiPaxosLeaderChange)
      newLeader := MultiPaxosLeader{}
      newLeader.epoch = mplc.NewEpoch
      newLeader.id = mplc.ID
      newLeader.numPingsMissed = 0
      mpx.leader = newLeader
      mpx.px.updateEpoch(mplc.NewEpoch)
      mpx.transition = false
  }
  mpx.results[executionPointer] = mop
}
func (mpx *MultiPaxos) initiateLeaderChange(){
  mpx.mu.Lock()
  defer mpx.mu.Unlock()

  currentEpoch = mpx.leader.epoch
  mpl := MultiPaxosLeaderChange{currentEpoch+1,mpx.me}
  mop := MultiPaxosOP{}
  mop.Type = LCHANGE
  mop.EpochNum = currentEpoch+1
  mop.Op = mpl
  mpx.transition = true
  mpx.px.Start(kv.executionPointer,mop)

}
//procedure run in go routine in the background that 
//checks the status of the Paxos instance pointed to by the
//executionPointer and then if agreement occured, the agreed upon
//operation is processed: applied, and the corresponding client request saved, and
//the executionPointer incremented.
//Essentially, it applies Paxos log entries in order, one at a time.
func (mpx *MultiPaxos) refresh(){

  //initial backoff time between status checks
  to := 10*time.Millisecond

  //while the server is still alive
  for mpx.dead == false{

    if(mpx.transition){
      time.Sleep(time.Second)
    }
    //check the status for the next Paxos instance in the Paxos log
    done,val := mpx.px.Status(mpx.executionPointer)

    //if agreement occurred
    if done{

      //commit and log the result of the instance (apply it and saved the result so the client
      //that made the request can get the result)
      mpx.commitAndLogInstance(mpx.executionPointer,val)

      to = 10*time.Millisecond
    }else{
      if(!mpx.isInit() || mpx.leader.numPingsMissed > NPINGS){
        mpx.initiateLeaderChange()
      }
      if(to < 5*time.Second){
        to = 2*to
      }
      time.Sleep(to)
    }
  }
}
func (mpx *MultiPaxos) ping(){
}
func (mpx *MultiPaxos) getDumpOfInstance() string{
  return ""
}
func (mpx *MultiPaxos) loadFromDump(dump string){

}
func (mpx *MultiPaxos) HandlePing(args *PingArgs, reply *PingReply){

}
//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *MultiPaxos {
  mpx := &MultiPaxos{}
  mpx.peers = peers
  mpx.me = me
  mpx.executionPointer=1
  mpx.leader = MultiPaxosLeader{0,me}
  mpx.transition = false
  mpx.result = make(map[int]*MultiPaxosOP)
  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(mpx)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(mpx)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    mpx.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for mpx.dead == false {
        conn, err := mpx.l.Accept()
        if err == nil && mpx.dead == false {
          if mpx.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if mpx.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            mpx.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            mpx.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && mpx.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }
  mpx.px = dpaxos.Make(peers,me,rpcs)
  return mpx
}