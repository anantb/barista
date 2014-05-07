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
import "sort"
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
  transition bool
  results map[int]*MultiPaxosOP
}
func (mpx *MultiPaxos) isLeaderAndValid() bool{
  l := mpx.leader
  if l.id==mpx.me && l.isValid(){
    return true
  }
  return false
}
func (mpx *MultiPaxos) isLeader() bool{
  l := mpx.leader
  if l.id==mpx.me{
    return true
  }
  return false
}
func (mpx *MultiPaxos) LeaderStart(seq int, v MultiPaxosOP){
    mpx.Log(0,"LeaderStart: Started proposal as leader "+strconv.Itoa(seq))
    mpx.Log(0,"LeaderStart: epoch"+strconv.Itoa(mpx.leader.epoch))
    failCallback := func(){
      mpx.Log(-10,"Explicit fail detected!")
      mpx.mu.Lock()
      mpx.leader.valid = false
      mpx.mu.Unlock()
    }
    mpx.px.FastPropose(seq,v,mpx.peers,failCallback)
    mpx.Log(0,"LeaderStart: epoch"+strconv.Itoa(mpx.leader.epoch)+" Fast propose done")
}
func (mpx *MultiPaxos) Start(seq int, v interface{}) (bool,string){
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  mpx.Log(1,"Start: Start called on instance "+strconv.Itoa(seq))
  if(seq < 0 || mpx.dead){
    mpx.Log(1,"Start: Start returned invalid isntance for "+strconv.Itoa(seq))
    return false, INVALID_INSTANCE
  }
  _,ok:=mpx.results[seq]
  if ok{
    return true, OK
  }
  if mpx.leader.isValid(){
    if mpx.isLeader(){
      //needed so don't do accept on something that has already been accepted
      if seq >= mpx.executionPointer{
        mop := MultiPaxosOP{}
        mop.Type = NORMAL
        mop.Op = v
        
        //Old Start: mpx.px.Start(seq,mop)
        //New Start:
        go mpx.LeaderStart(seq,mop)
        mpx.Log(1,"Start: agreement started for "+strconv.Itoa(seq))
      }
      return true,OK
    }else{
      mpx.Log(1,"Start: not leader "+strconv.Itoa(seq))
      go mpx.remoteStart(seq,v)
      return true, OK
    }
  }
  return false, UNKNOWN_LEADER
}


func (mpx *MultiPaxos) Done(seq int) {
  mpx.px.Done(seq)
}


func (mpx *MultiPaxos) Max() int {
  return mpx.executionPointer
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
func (mpx *MultiPaxos) remoteStart(seq int, v interface{}){
  done := false
  mpx.mu.Lock()
  leader := mpx.leader
  mpx.mu.Unlock()
  leaderAddr := mpx.peers[leader.id]
  for !done{
    args := &RemoteStartArgs{}
    args.InstanceNumber = seq
    args.Op = v
    reply := &RemoteStartReply{}
    ok := false
    if leaderAddr == mpx.peers[mpx.me]{
      mpx.RemoteStart(args,reply)
    }else{
      ok=call(leaderAddr, "MultiPaxos.RemoteStart", args, reply)
    }
    if ok{
      switch reply.Status{
      case OK:
        done = true
      case NOT_LEADER:
        if reply.Leader != "" && reply.Epoch >= leader.epoch{
          mpx.getInstancesFromReplica(reply.Leader,true)
        }
        //keep waiting, need to make sure leader gets this
      }
    }
  }
}
//generates a new instance number for the incoming client request
//this instance number is the instance used for paxos agreement
func (mpx *MultiPaxos) getInstanceNum() int{
  executionPointerSample := mpx.executionPointer
  return executionPointerSample
}

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
    mpx.Log(1,"PAAW: Before Start on instance "+strconv.Itoa(instanceNum))
    mpx.px.Start(instanceNum,mop)
    mpx.Log(1,"PAAW: After Start on instance "+strconv.Itoa(instanceNum))
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
    mpx.Log(1,"PAAW: After agree on instance "+strconv.Itoa(instanceNum))
    //figure out termination condition
    ok,agreedOp:= mpx.px.Status(instanceNum)
    if ok{
      mAgreedOp := agreedOp.(MultiPaxosOP)
      switch mAgreedOp.Type{
        case LCHANGE:
          return mAgreedOp
      }
    }
    if !mpx.transition{
      return nil
    }
  }
  return nil
}
func (mpx *MultiPaxos) getInstanceData(lowest int) map[int]MultiPaxosOP{
  instancesd := make(map[int]MultiPaxosOP)
  for instanceNum,data := range mpx.results{
    if instanceNum >= lowest{
      instancesd[instanceNum] = *data
    }
  }
  return instancesd
}
func (mpx *MultiPaxos) commitAndLogMany(InstancesData map[int]MultiPaxosOP){
  mpx.Log(0,"getInstancesFromLeader: got data")
  //sort instance data
  instanceNums := make(intarr,len(InstancesData),len(InstancesData))
  index := 0
  for instanceNum,_ := range InstancesData{
    instanceNums[index] = instanceNum
    index++
  }

  //now apply instance data in order
  sort.Sort(instanceNums)
  for _,instanceNum := range instanceNums{
    //make sure no gaps and truly in order
    if instanceNum == mpx.executionPointer{
      data,_:= InstancesData[instanceNum]
      mpx.commitAndLogInstance(instanceNum,data)
    }
  }
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
  if executionPointer != mpx.executionPointer{
    mpx.Log(-10,"Commit and Log: "+strconv.Itoa(executionPointer)+"  bad execution pointer")
    mpx.executionPointer--
    return
  }
  mpx.Log(1,"Commit and Log: "+strconv.Itoa(executionPointer)+"  before anything")
  //currentLeader := mpx.leader
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
      newLeader.valid = true
      mpx.leader = newLeader
      mpx.transition = false
      mpx.px.SetLeaderAndEpoch(mpx.peers[newLeader.id], newLeader.epoch)
      mpx.Log(1,"Commit and Log: "+strconv.Itoa(executionPointer)+"  found leader change")
      mpx.Log(1,"Commit and Log: "+strconv.Itoa(executionPointer)+"  new epoch"+strconv.Itoa(mplc.NewEpoch))
      mpx.Log(1,"Commit and Log: "+strconv.Itoa(executionPointer)+"  new leader"+mpx.peers[mplc.ID])
  }
  mpx.results[executionPointer] = &mop
  mpx.Log(1,"Commit and Log: done")
}
func (mpx *MultiPaxos) initiateLeaderChange(){
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  //double check
  mpx.Log(-10,"LeaderChange: before leader change")
  if !mpx.leader.isValid() && !mpx.transition{
    mpx.Log(1,"LeaderChange: starting leader change")
    currentEpoch := mpx.leader.epoch
    mpl := MultiPaxosLeaderChange{currentEpoch+1,mpx.me}
    mop := MultiPaxosOP{}
    mop.Type = LCHANGE
    mop.Op = mpl
    mpx.transition = true
    //tell underlying paxos to not accept messages from old leader
    mpx.px.UpdateEpoch(currentEpoch+1)
  
    go func(){
      maxInd := mpx.me
      for i:=mpx.me ; i<len(mpx.peers);i++{
        peerAddr := mpx.peers[i]
        args := &PingArgs{}
        args.LowestInstance = mpx.executionPointer
        reply :=  &PingReply{}

        if i == mpx.me{
          continue
        }
        ok := call(peerAddr, "MultiPaxos.HandlePing", args, reply)
        mpx.commitAndLogMany(reply.InstancesData)
        mpx.Log(-10,"got data from"+peerAddr)
        if ok{
          if i > maxInd{
            maxInd = i
          }
        }
        mpx.Log(-1,"Max was "+strconv.Itoa(maxInd))
      }
      mpx.Log(-1,"Max was "+strconv.Itoa(maxInd))
      if mpx.me == maxInd{
        mpx.Log(-1,"Max was me")
        mpx.startPaxosAgreementAndWait(mop)
        log.Println("Max was me "+mpx.peers[mpx.me])
      }
    }()
  }
  mpx.Log(1,"LeaderChange: leader change started")
}
func (mpx *MultiPaxos) ping(){
  mpx.mu.Lock()
  l := mpx.leader
  executionPointer := mpx.executionPointer
  mpx.mu.Unlock()

  //no need to ping yourself
  if mpx.isLeader() && !mpx.leader.isValid(){
    return
  }

  //otherwise ping actual leader
  leaderAddr := mpx.peers[l.id]
  args := &PingArgs{}
  args.LowestInstance = executionPointer
  reply :=  &PingReply{}
  ok := call(leaderAddr, "MultiPaxos.HandlePing", args, reply)

  if(!ok){
    mpx.mu.Lock()
    l.numPingsMissed +=1
    mpx.mu.Unlock()
  }else{
    switch reply.Status{
    case OK:
      l.numPingsMissed = 0
      //nothing
      mpx.commitAndLogMany(reply.InstancesData)
    case NOT_LEADER:
      //do something
      mpx.mu.Lock()
      mpx.leader.valid = false
      mpx.mu.Unlock()
      mpx.getInstancesFromReplica(reply.Leader,true)
    }
  }
}
func (mpx *MultiPaxos) findLeader() string{
  
  highestEpoch := 0
  highestLeader := ""

  if mpx.isLeader() && mpx.leader.isValid(){
    return mpx.peers[mpx.leader.id]
  }

  for _,serverAddr := range mpx.peers{
    args := &PingArgs{}
    reply :=  &PingReply{}
    ok := false
    count := 0

    //fix magic number
    for !ok && count <5{
      ok = call(serverAddr, "MultiPaxos.HandlePing", args, reply)
      count++
    }
    if ok{
      returnedEpoch := reply.Epoch
      returnedLeader := reply.Leader
      if returnedEpoch >= highestEpoch{
        highestEpoch = returnedEpoch
        highestLeader = returnedLeader
      }
    }
  }
  return highestLeader
}
func (mpx *MultiPaxos) getInstancesFromReplica(leader string, forceLeader bool){
  //make rpc to leader
  leaderAddr := leader
  
  if mpx.isLeader() && mpx.leader.isValid(){
    return
  }

  done := false
  for !mpx.dead && !done{
    args := &PingArgs{}
    mpx.mu.Lock()
    args.LowestInstance = mpx.executionPointer
    mpx.mu.Unlock()
    reply :=  &PingReply{}
    ok := call(leaderAddr, "MultiPaxos.HandlePing", args, reply)

    //if rpc succeeded then update records
    if ok{
        switch reply.Status{
        case OK:
          mpx.commitAndLogMany(reply.InstancesData)
          done = true
        case NOT_LEADER:
          //do something
          if !forceLeader{
            mpx.commitAndLogMany(reply.InstancesData)
            done = true
          }
          mpx.Log(0,"getInstancesFromLeader: got no leader")
          leaderAddr = reply.Leader
          mpx.Log(0,"getInstancesFromLeader: found new leader"+leaderAddr)
        }
    }
  }
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
    mpx.mu.Unlock()
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
      if !mpx.leader.isValid(){
          mpx.Log(-10,"refresh: initiating failover "+strconv.Itoa(executionPointer))
          leader := mpx.findLeader()
          if leader == "" || leader == mpx.peers[mpx.me]{
            mpx.Log(-10,"no leader found, starting selection")
            mpx.initiateLeaderChange()
          }else{
            mpx.getInstancesFromReplica(leader,true)
          }
      }
      if(to < 2*time.Second){
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
    mpx.Log(1,"Pings missed:"+strconv.Itoa(mpx.leader.numPingsMissed))
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
  reply.InstancesData = mpx.getInstanceData(args.LowestInstance)
  reply.Epoch = mpx.leader.epoch
  if mpx.leader.isValid(){
    reply.Leader = mpx.peers[mpx.leader.id]
    if mpx.isLeader(){
      reply.Status = OK
    }else{
      reply.Status = NOT_LEADER
    }
  }else{
    reply.Status = NOT_LEADER
    reply.Leader = ""
  }
  return nil
}
func (mpx *MultiPaxos) RemoteStart(args *RemoteStartArgs, reply *RemoteStartReply) error{
  mpx.mu.Lock()
  defer mpx.mu.Unlock()
  reply.Epoch = mpx.leader.epoch
  if mpx.leader.isValid(){
    reply.Leader = mpx.peers[mpx.leader.id]
    if mpx.isLeader(){
      reply.Status = OK
      go func(){
          mpx.Start(args.InstanceNumber,args.Op)
        }()
    }else{
      reply.Status = NOT_LEADER
    }
  }else{
    reply.Status = NOT_LEADER
    reply.Leader = ""
  }
  return nil
}
func (mpx *MultiPaxos) GetRPCCount() int{
  return mpx.rpcCount + mpx.px.GetRPCCount()
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
  gob.Register(RemoteStartArgs{})
  gob.Register(RemoteStartReply{})
  mpx := &MultiPaxos{}
  mpx.peers = peers
  mpx.me = me
  mpx.executionPointer = -1
  mpx.leader = &MultiPaxosLeader{0,me,0,false}
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