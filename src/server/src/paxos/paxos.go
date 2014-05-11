package paxos

/**
 * Paxos Library
 *
 * @author: Anant Bhardwaj
 * @date: 03/23/2014
 */

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
import "time"
import "storage"
import "encoding/json"
import "strconv"
import "strings"

type Paxo struct {
  n_p int64
  n_a int64
  v_a interface{}
  decided bool
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  // Your data here.
  majority int
  max_seq int
  done map[string]int
  store map[int]*Paxo
  paxos_lock sync.Mutex
  unix bool
  path string
  sm *storage.StorageManager
  use_zookeeper bool
}

const (
  OK = "OK"
  REJECT = "REJECT"
)

const PORT = ":9001"

type status string

type PrepareArgs struct {
  Me string
  N int64
  Seq int
  Done int
}

type PrepareReply struct {
  Status status
  N_A int64
  Value interface{}
  Done int
}

type AcceptArgs struct {
  Me string
  N_A int64
  Seq int
  Value interface{}
  Done int
}

type AcceptReply struct {
  Status status
  Done int
}

type DecidedArgs struct {
  Me string
  Seq int
  Value interface{}
  Done int
}

type DecidedReply struct {
  Status status
  Done int
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  var paxo *Paxo
  var ok bool

  if px.use_zookeeper {
    paxo, ok = px.Read(px.path + "/store/" + strconv.Itoa(args.Seq))
  } else {
    paxo, ok = px.store[args.Seq]
  }
  
  if !ok {
    paxo = &Paxo{n_p: -1, n_a: -1, v_a: nil, decided: false}
  }

  if args.N > paxo.n_p {
    paxo.n_p = args.N
    reply.Status = OK
    reply.N_A = paxo.n_a
    reply.Value = paxo.v_a
  } else {
    reply.Status = REJECT
  }

  if px.use_zookeeper {
    px.WriteS(px.path + "/done/" + px.Format(px.peers[px.me]), strconv.Itoa(args.Done))
    px.Write(px.path + "/store/" + strconv.Itoa(args.Seq), paxo)
    data, _ := px.ReadS(px.path + "/done/" + px.Format(px.peers[px.me]))
    reply.Done, _ = strconv.Atoi(data)
  } else {
    px.done[args.Me] = args.Done
    px.store[args.Seq] = paxo
    reply.Done = px.done[px.peers[px.me]]
  }
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  var paxo *Paxo
  var ok bool

  if px.use_zookeeper {
    paxo, ok = px.Read(px.path + "/store/" + strconv.Itoa(args.Seq))
  } else {
    paxo, ok = px.store[args.Seq]
  }
  
  if !ok {
    paxo = &Paxo{n_p: -1, n_a: -1, v_a: nil, decided: false}
  }

  if args.N_A >= paxo.n_p {
    paxo.n_p = args.N_A
    paxo.n_a = args.N_A
    paxo.v_a = args.Value
    reply.Status = OK
  } else {
    reply.Status = REJECT
  }

  if px.use_zookeeper {
    px.WriteS(px.path + "/done/" + px.Format(px.peers[px.me]), strconv.Itoa(args.Done))
    px.Write(px.path + "/store/" + strconv.Itoa(args.Seq), paxo)
    data, _ := px.ReadS(px.path + "/done/" + px.Format(px.peers[px.me]))
    reply.Done, _ = strconv.Atoi(data)
  } else {
    px.done[args.Me] = args.Done
    px.store[args.Seq] = paxo
    reply.Done = px.done[px.peers[px.me]]
  }

  return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  var paxo *Paxo
  var ok bool

  if px.use_zookeeper {
    paxo, ok = px.Read(px.path + "/store/" + strconv.Itoa(args.Seq))
  } else {
    paxo, ok = px.store[args.Seq]
  }

  if !ok {
    paxo = &Paxo{n_p: -1, n_a: -1, v_a: nil, decided: false}
  }

  paxo.v_a = args.Value
  paxo.decided = true

  reply.Status = OK
  if px.use_zookeeper {
    px.WriteS(px.path + "/done/" + px.Format(px.peers[px.me]), strconv.Itoa(args.Done))
    px.Write(px.path + "/store/" + strconv.Itoa(args.Seq), paxo)
    data, _ := px.ReadS(px.path + "/done/" + px.Format(px.peers[px.me]))
    reply.Done, _ = strconv.Atoi(data)
  } else {
    px.done[args.Me] = args.Done
    px.store[args.Seq] = paxo
    reply.Done = px.done[px.peers[px.me]]
  }
  return nil
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
func call(srv string, name string, args interface{}, reply interface{}, unix bool) bool {
  var err error
  var c *rpc.Client
  if unix {
    c, err = rpc.Dial("unix", srv)
  } else {
    c, err = rpc.Dial("tcp", srv)
  }
 
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

func (px *Paxos) generate_id() int64 {
  return time.Now().UnixNano()
}


func (px *Paxos) Propose(seq int, v interface{}) {
  // Your code here.
  px.mu.Lock()
  if seq > px.max_seq {
    px.max_seq = seq
  }

  var paxo *Paxo
  var ok bool

  if px.use_zookeeper {
    paxo, ok = px.Read(px.path + "/store/" + strconv.Itoa(seq))
  } else {
    paxo, ok = px.store[seq]
  }

  if !ok {
    paxo = &Paxo{n_p: -1, n_a: -1, v_a: nil, decided: false}
    px.store[seq] = paxo
  }

  if px.use_zookeeper {
    px.Write(px.path + "/store/" + strconv.Itoa(seq), paxo)
  } else {
    px.store[seq] = paxo
  }

  px.mu.Unlock()
  me := px.peers[px.me]
  t_wait := 50 * time.Millisecond
  for px.dead == false && paxo.decided == false {
    n := px.generate_id()
    highest_n_a := int64(-1)
    highest_v_a := v
    prepare_ok_count := 0
    accept_ok_count := 0

    for _, peer := range px.peers {
      //fmt.Printf ("Prepare: %v,%v -- %v\n", n, seq, v)
      prepare_args := PrepareArgs {Me:me, N: n, Seq: seq, Done: px.done[me]}
      var prepare_reply PrepareReply
      if peer != me {
        call(peer, "Paxos.Prepare", &prepare_args, &prepare_reply, px.unix)
      } else {
        px.Prepare(&prepare_args, &prepare_reply)
      }
      if prepare_reply.Status == OK {
        prepare_ok_count += 1
        if prepare_reply.N_A > highest_n_a {
          highest_n_a = prepare_reply.N_A
          highest_v_a = prepare_reply.Value
        }
        if px.use_zookeeper {
          px.WriteS(px.path + "/done/" + px.Format(peer), strconv.Itoa(prepare_reply.Done))
        }else {
          px.done[peer] = prepare_reply.Done
        } 
      }
    }

    if prepare_ok_count < px.majority {
      time.Sleep(t_wait)
      continue
    }

    if highest_v_a == nil {
      highest_v_a = v
    }

    //fmt.Printf ("Accept: %v,%v -- %v\n", n, seq, highest_v_a)
    for _, peer := range px.peers {
      accept_args := AcceptArgs {Me:me, N_A: n, Seq: seq, Value: highest_v_a, Done: px.done[me]}
      var accept_reply AcceptReply
      if peer != me {
        call(peer, "Paxos.Accept", &accept_args, &accept_reply, px.unix)
      } else {
        px.Accept(&accept_args, &accept_reply)
      }
      if accept_reply.Status == OK {
        accept_ok_count += 1
        if px.use_zookeeper {
          px.WriteS(px.path + "/done/" + px.Format(peer), strconv.Itoa(accept_reply.Done))
        }else {
          px.done[peer] = accept_reply.Done
        } 
      }
    }

    if accept_ok_count < px.majority  {
      time.Sleep(t_wait)
      continue
    }

    //fmt.Printf ("Decide: %v,%v -- %v\n", n, seq, highest_v_a)
    for _, peer := range px.peers {
      decided_args := DecidedArgs {Me:me, Seq: seq, Value: highest_v_a, Done: px.done[me]}
      var decided_reply DecidedReply
      if peer != me {
        call(peer, "Paxos.Decided", &decided_args, &decided_reply, px.unix)
      } else {
        px.Decided(&decided_args, &decided_reply)
      }
      if px.use_zookeeper {
        px.WriteS(px.path + "/done/" + px.Format(peer), strconv.Itoa(decided_reply.Done))
      }else {
        px.done[peer] = decided_reply.Done
      }     
    }

  }
}

func (px *Paxos) ClearMemory(seq int) {
  for k, _ := range(px.store) {
    if k < seq {
      if px.use_zookeeper {
        px.Delete(px.path + "/store/" + strconv.Itoa(seq))
      } else {
        delete(px.store, k)
      }
    }
  }
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  if seq >= px.Min() {
    go px.Propose(seq, v)
  }
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  peer := px.peers[px.me]

  sq_done := 0

  if px.use_zookeeper {
    data, ok := px.ReadS(px.path + "/done/" + peer)
    if ok {
      sq_done, _ = strconv.Atoi(data)
    }
  } else {
    sq_done = px.done[peer]
  }
  if sq_done < seq {
    if px.use_zookeeper {
      px.WriteS(px.path + "/done/" + peer, strconv.Itoa(seq))
    }else {
      px.done[peer] = seq
    }
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  return px.max_seq
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
  // You code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  min := math.MaxInt32
  for _, v := range(px.done) {
    if v < min {
      min = v
    }
  }

  min = min + 1
  px.ClearMemory(min)

  return min
}

func (px *Paxos) Format(path string) string {
  return strings.Replace(path, "/", "_", -1)
}

func (px *Paxos) CreateS(path string, data string) {
  px.sm.Create(path, data)
}

func (px *Paxos) WriteS(path string, data string) {
  px.sm.Write(path, data)
}

func (px *Paxos) ReadS(path string) (string, bool) {
  data, err := px.sm.Read(path)

  if err != nil {
    return "", false
  }

  return data, true
}

func (px *Paxos) DeleteS(path string){
  px.sm.Delete(path)
}


func (px *Paxos) Create(path string, paxo *Paxo) {
  data, err := json.Marshal(paxo)
  fmt.Printf("Error %v\n", err)
  data_str := string(data)
  fmt.Printf("create path: %v, pax: %v, data: %v, data_str: %v\n", path, paxo, data, data_str)
  px.CreateS(path, data_str)
}

func (px *Paxos) Write(path string, paxo *Paxo) {
  data, err := json.Marshal(paxo)
  fmt.Printf("Error %v\n", err)
  data_str := string(data)
  fmt.Printf("write path: %v, pax: %v, data: %v, data_str: %v\n", path, paxo, data, data_str)
  px.WriteS(path, data_str)
}

func (px *Paxos) Read(path string) (*Paxo, bool) {
  data, ok := px.ReadS(path)

  if !ok {
    return nil, ok
  }

  var paxo Paxo
  json.Unmarshal([]byte(data), &paxo)
  return &paxo, ok
}

func (px *Paxos) Delete(path string){
  px.DeleteS(path)
}
//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  min := px.Min()
  px.mu.Lock()
  defer px.mu.Unlock()

  var paxo *Paxo
  var ok bool

  if px.use_zookeeper {
    paxo, ok = px.Read(px.path + "/store/" + strconv.Itoa(seq))
  } else {
    paxo, ok = px.store[seq]
  }

  if ok && seq >= min && paxo != nil && paxo.decided == true {
    return true, paxo.v_a
  }
  return false, nil
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
  if px.use_zookeeper && px.sm != nil {
    px.sm.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
// @mvartak: port is now included in peer addresses
func Make(peers []string, me int, rpcs *rpc.Server, unix bool) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.unix = unix

  // Your initialization code here.
  px.majority = (len(peers) / 2) + 1
  px.max_seq = -1
  px.store = make(map[int]*Paxo)
  px.done = make(map[string]int)
  px.done[peers[me]] = -1

  px.use_zookeeper = false

  if px.use_zookeeper {
    px.sm = storage.MakeStorageManager()
    px.sm.Open("localhost:2181")

    px.CreateS("/paxos", "")
    px.path = "/paxos/" + px.Format(px.peers[px.me])
    px.CreateS(px.path, "")
    px.CreateS(px.path + "/store", "")
    px.CreateS(px.path + "/done", "")

    for _, peer := range px.peers {
      px.CreateS(px.path + "/done/" + px.Format(peer), "0")
    }    
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

  var l net.Listener
  var e error
  if unix {
    os.Remove(peers[me])
    l, e = net.Listen("unix", peers[me])
  } else {
    l, e = net.Listen("tcp", peers[me])
  }

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

            if unix {
              c1 := conn.(*net.UnixConn)
              f, _ := c1.File()
              err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
              if err != nil {
                fmt.Printf("shutdown: %v\n", err)
              }
              px.rpcCount++
              go rpcs.ServeConn(conn)
            } else {
              c1 := conn.(*net.TCPConn)
              f, _ := c1.File()
              err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
              if err != nil {
                fmt.Printf("shutdown: %v\n", err)
              }
              px.rpcCount++
              go rpcs.ServeConn(conn)
            }
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
