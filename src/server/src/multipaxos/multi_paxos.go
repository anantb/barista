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
//import "time"
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
}


func (mpx *MultiPaxos) Start(seq int, v interface{}) (bool,string){
  mpx.px.Start(seq,v)
  return false,""
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
  return mpx.px.Status(seq)
}

func (mpx *MultiPaxos) Kill() {
  mpx.dead = true
  if mpx.l != nil {
    mpx.l.Close()
  }
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