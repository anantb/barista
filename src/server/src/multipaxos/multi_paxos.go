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
import "strconv"
import "time"
import "encoding/gob"
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
  leader *MultiPaxosLeader
  executionPointer int
  instanceNum int
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
func (mpx *MultiPaxos) LeaderStart(seq int, v MultiPaxosOP){
    mpx.Log(1,"LeaderStart: Started proposal as leader "+strconv.Itoa(seq))
    mpx.Log(1,"LeaderStart: epoch"+strconv.Itoa(v.EpochNum))
    mpx.px.FastPropose(seq,v,mpx.peers)
}
func (mpx *MultiPaxos) Start(seq int, v interface{}) (bool,string,string){
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  mpx.Log(1,"Start: Start called on instance "+strconv.Itoa(seq))
  if(seq < 0){
    mpx.Log(1,"Start: Start returned invalid isntance for "+strconv.Itoa(seq))
    return false, INVALID_INSTANCE, ""
  }
  if mpx.isLeader(){
    //needed so don't do accept on something that has already been accepted
    if seq > mpx.executionPointer-1{
      mop := MultiPaxosOP{}
      mop.Type = NORMAL
      mop.EpochNum = mpx.leader.epoch
      mop.Op = v
      
      //Old Start: mpx.px.Start(seq,mop)
      //New Start:
      go mpx.LeaderStart(seq,mop)
      mpx.Log(1,"Start: agreement started for "+strconv.Itoa(seq))
    }
    return true,OK, ""
  }
  if(mpx.isInit()){
    mpx.Log(1,"Start: not leader "+strconv.Itoa(seq))
    return false,WRONG_SERVER, mpx.peers[mpx.leader.id]
  }else{
    mpx.Log(1,"Start: waiting for init "+strconv.Itoa(seq))
    for !mpx.isInit(){
      //loop spin, maybe do something better here
      time.Sleep(100)
    }
    mpx.Log(1,"Start: actually starting "+strconv.Itoa(seq))
    return mpx.Start(seq,v)
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
  mpx.mu.Lock()
  defer mpx.mu.Unlock()

  //don't return results of instance, before we know the results of previous instances
  if seq >= mpx.executionPointer{
    return false, nil
  }

  val,ok := mpx.results[seq]
  if ok{
    switch val.Op.(type){
      case MultiPaxosLeaderChange:
        return ok,nil
      default:
        return ok,val.Op
    }
  }
  return ok,nil
}
func (mpx *MultiPaxos) SetUnreliable(unreliable bool){
  mpx.mu.Lock()
  defer mpx.mu.Unlock()

  mpx.unreliable = unreliable
  mpx.px.SetUnreliable(unreliable)
}
func (mpx *MultiPaxos) Kill() {
  mpx.mu.Lock()
  defer mpx.mu.Unlock()

  mpx.dead = true
  if mpx.l != nil {
    mpx.l.Close()
  }
  mpx.px.Kill()
}
//generates a new instance number for the incoming client request
//this instance number is the instance used for paxos agreement
func (mpx *MultiPaxos) getInstanceNum() int{
  mpx.instanceNum +=1
  executionPointerSample := mpx.executionPointer

  //the new instance number generated must be larger than the execution pointer
  //otherwise the operation will fail to be agreed upon since you are trying to propose
  //a new op fzr a instance (slot in the log) that was already agreed upon
  if executionPointerSample > mpx.instanceNum{
      mpx.instanceNum += (executionPointerSample - mpx.instanceNum)
  }
  return mpx.instanceNum
}
//called by Shard Master RPC handlers with the appropriate Op object
//(that the RPC Handlers create) and then Paxos agreement is initated on
//that Op object until it is agreed upon and applied to the shard master server.
//This method blocks until agreement for the proposed op succeeds and the 
//corresponding response is generated. This method also handles retrying Paxos
//agreement with a new Instance number if the Op lost agreement on its old instance
//number
func (mpx *MultiPaxos) startPaxosAgreementAndWait(mop MultiPaxosOP) interface{}{
  mpx.mu.Lock()
  defer mpx.mu.Unlock()

  //boolean indicating that agreement and response generation is
  //done
  done := false

  //while not done or the server is not dead
  for !done && !mpx.dead{

    //generate new instance number
    instanceNum := mpx.getInstanceNum()

    //start paxos agreement for this op for the 
    //instance number generated above
    mpx.px.Start(instanceNum,mop)

    //boolean representing the completion of paxos
    //agreement and application of the agreed upon
    //op to the server
    pxDone := false

    //while Paxos agreement and operation application
    //is not done and the server is not dead
    for !pxDone && !mpx.dead{

      //if the execution pointer of the server
      //passes the instance number of the Paxos agreement
      //for this op, then agreement finished for that instance
      //and the result of agreement was applied to the server
      //so break
      if mpx.executionPointer > instanceNum{
        pxDone = true
      }

      //release the lock so that other threads, including the
      //server refresh/execution thread, can run and all can 
      //make progress while still blocking
      mpx.mu.Unlock()

      //allow thread to sleep before checking again so that other 
      //threads can run
      time.Sleep(10*time.Millisecond)

      //reacquire lock so that checks and updates
      //to internal data structures are done under mutex
      //for the next iteration of the loop or after the loop
      mpx.mu.Lock()
    }
    //figure out termination condition
    return nil
  }
  return nil
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
  mpx.Log(1,"Commit and Log: "+strconv.Itoa(executionPointer)+"  before anything")
  currentLeader := mpx.leader
  //at most once RPC
  mop := val.(MultiPaxosOP)
  switch(mop.Type){
    case NORMAL:
      mpx.Log(1,"Commit and Log: "+strconv.Itoa(executionPointer)+"  found normal op")
      //do nothing
    case LCHANGE:
      mplc := mop.Op.(MultiPaxosLeaderChange)
      newLeader := &MultiPaxosLeader{}
      newLeader.epoch = mplc.NewEpoch
      newLeader.id = mplc.ID
      newLeader.numPingsMissed = 0
      mpx.leader = newLeader
      mpx.px.UpdateEpoch(mplc.NewEpoch)
      mpx.transition = false
      mpx.Log(1,"Commit and Log: "+strconv.Itoa(executionPointer)+"  found leader change")
      mpx.Log(1,"Commit and Log: "+strconv.Itoa(executionPointer)+"  new epoch"+strconv.Itoa(mplc.NewEpoch))
      mpx.Log(1,"Commit and Log: "+strconv.Itoa(executionPointer)+"  new leader"+mpx.peers[mplc.ID])
  }
  if mop.EpochNum >= currentLeader.epoch{
    mpx.results[executionPointer] = &mop
  }else{
    mpx.Log(1,"invalid operation, cannot expose the results of an old leader after the leader change")
    mpx.executionPointer--
  }
  mpx.Log(1,"Commit and Log: done")
}
func (mpx *MultiPaxos) initiateLeaderChange(){
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  //double check
  mpx.Log(1,"LeaderChange: before leader change")
  if(mpx.leader.epoch<=0 || mpx.leader.numPingsMissed > NPINGS){
    mpx.Log(1,"LeaderChange: starting leader change")
    currentEpoch := mpx.leader.epoch
    mpl := MultiPaxosLeaderChange{currentEpoch+1,mpx.me}
    mop := MultiPaxosOP{}
    mop.Type = LCHANGE
    mop.EpochNum = currentEpoch
    mop.Op = mpl
    mpx.transition = true
    mpx.px.Start(mpx.executionPointer,mop)
  }
}
func (mpx *MultiPaxos) ping(){
  mpx.mu.Lock()
  l := mpx.leader
  mpx.mu.Unlock()
  
  leaderAddr := mpx.peers[l.id]
  args := &PingArgs{}
  reply :=  &PingReply{}
  ok := call(leaderAddr, "MultiPaxos.HandlePing", args, reply)

  mpx.mu.Lock()
  if(!ok){
    l.numPingsMissed +=1
  }
  mpx.mu.Unlock()
}
//procedure run in go routine in the background that 
//checks the status of the Paxos instance pointed to by the
//executionPointer and then if agreement occured, the agreed upon
//operation is processed: applied, and the corresponding client request saved, and
//the executionPointer incremented.
//Essentially, it applies Paxos log entries in order, one at a time.
func (mpx *MultiPaxos) refresh(){

  //initial backoff time between status checks
  to := 40*time.Millisecond
  //while the server is still alive
  dead := false
  for dead== false{
    mpx.mu.Lock()
    dead = mpx.dead
    executionPointer := mpx.executionPointer
    transition := mpx.transition
    mpx.mu.Unlock()
    if(transition){
      time.Sleep(time.Second)
    }
    mpx.Log(1,"refresh: checking "+strconv.Itoa(executionPointer))
    //check the status for the next Paxos instance in the Paxos log
    done,val := mpx.px.Status(executionPointer)
    mpx.Log(1,"refresh: after checking "+strconv.Itoa(executionPointer))
    //if agreement occurred
    if done{
      mpx.Log(1,"refresh: before commitAndLog "+strconv.Itoa(executionPointer))
      //commit and log the result of the instance (apply it and saved the result so the client
      //that made the request can get the result)
      mpx.commitAndLogInstance(executionPointer,val)

      to = 10*time.Millisecond
    }else{
      mpx.ping()
      if(!mpx.isInit() || mpx.leader.numPingsMissed > NPINGS){
        mpx.Log(1,"refresh: initiating failover "+strconv.Itoa(executionPointer))
        mpx.initiateLeaderChange()
      }
      if(to < 3*time.Second){
        to = 2*to
      }
      time.Sleep(to)
    }
  }
}
func (mpx *MultiPaxos) refreshPing(){
    //initial backoff time between status checks
  to := PINGINTERVAL

  //while the server is still alive
  for mpx.dead == false{
    mpx.ping()
    //log.Printf("Pings missed:"+strconv.Itoa(mpx.leader.numPingsMissed))
    time.Sleep(to)
  }
}
func (mpx *MultiPaxos) getDumpOfInstance() string{
  return ""
}
func (mpx *MultiPaxos) loadFromDump(dump string){

}
func (mpx *MultiPaxos) HandlePing(args *PingArgs, reply *PingReply) error{
  mpx.mu.Lock()
  defer mpx.mu.Unlock()

  //to be removed
  mpx.rpcCount--
  //to be removed

  reply.Status = OK
  return nil
}
//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *MultiPaxos {
  gob.Register(MultiPaxosOP{})
  gob.Register(PingArgs{})
  gob.Register(PingReply{})
  gob.Register(MultiPaxosLeaderChange{})
  mpx := &MultiPaxos{}
  mpx.peers = peers
  mpx.me = me
  mpx.executionPointer=0
  mpx.instanceNum = 0
  mpx.leader = &MultiPaxosLeader{0,me,0}
  mpx.transition = false
  mpx.results = make(map[int]*MultiPaxosOP)
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
      dead := false
      for dead == false {
        mpx.mu.Lock()
        dead = mpx.dead
        mpx.mu.Unlock()
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
  go mpx.refreshPing()
  go mpx.refresh()

  return mpx
}
func (mpx *MultiPaxos) Log(pri int, msg string){
  Log(pri, "MultiPaxos", mpx.peers[mpx.me], msg)
}