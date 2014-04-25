package sqlpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "strconv"
import "math"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}


type Op struct {
  // Your definitions here.
  Args ExecArgs
  NoOp bool
  SeqNum int
  

  //Args interface {}
  //Reply interface {}
  //ClientId int64 // unique client id
  //RequestId int // sequential request id
  //NoOp bool // true if this is a no-op
  //Done bool // true if we can delete this data
}


type SQLPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  ops map[int]Op // log of operations
  data map[string]string // the database
  lastSeen map[int64]Op // the last request/reply for this client  	   		 
  next int // the next sequence number to be executed
}

func (sp *SQLPaxos) execute(args interface {}) interface {} {

   if args == nil {
      return nil
   }

   switch t := args.(type) {
   default:
      fmt.Printf("unexpected type %T\n", t)
   case PutArgs:
      // execute the put
      reply := PutReply{}
      putArgs := args.(PutArgs)
   
      prevValue, ok := sp.data[putArgs.Key]
      if ok {
         reply.PreviousValue = prevValue
      } else {
         reply.PreviousValue = ""
      }

      if putArgs.DoHash {
         sp.data[putArgs.Key] = strconv.Itoa(int(hash(reply.PreviousValue + putArgs.Value)))
      } else {
         sp.data[putArgs.Key] = putArgs.Value
      }

      reply.Err = OK

      return reply

   case GetArgs:
      // execute the get
      reply := GetReply{}
      key := args.(GetArgs).Key

      value, ok := sp.data[key]
      if ok {
         reply.Value = value
         reply.Err = OK          
      } else {
         reply.Value = ""
         reply.Err = ErrNoKey
      }

      return reply

   }

   return nil
}

func (sp *SQLPaxos) fillHoles(next int, seq int) interface{} {
 
  var reply interface {} = nil

  // make sure there are no holes in the log before our operation
  for i := next; i <= seq; i++ {
     nwaits := 0
     for !sp.dead {
	if _, ok := sp.ops[i]; ok || sp.next > i {
      	   break
        }

        decided, v_i := sp.px.Status(i)
        if decided {
           // the operation in slot i has been decided
           sp.ops[i] = v_i.(Op)
           break
        } else {
           nwaits++
           sp.mu.Unlock()
           if nwaits == 5 || nwaits == 10 {
              // propose a no-op
              sp.px.Start(i, Op{NoOp: true})
           } else if nwaits > 10 {
              time.Sleep(100 * time.Millisecond)
           } else {
              time.Sleep(10 * time.Millisecond)
           }
           sp.mu.Lock()
        }
     }

     if i == sp.next {
        // the operation at slot i is next to be executed
        op_i := sp.ops[i]
        if !op_i.NoOp {
           // execute the operation at slot i
	   r, executed := sp.checkIfExecuted(op_i.ClientId, op_i.RequestId)
           if executed {
	      op_i.Reply = r
	   } else {
	      r := sp.execute(op_i.Args)
	      op_i.Reply = r
	      sp.lastSeen[op_i.ClientId] = op_i
	   }
           sp.ops[i] = op_i
        }
        sp.next++
        if i == seq {
           reply = op_i.Reply
        }
     } else if i == seq {
        // our operation was already executed
        reply = sp.ops[seq].Reply
     }
  }

  return reply
} 

func (sp *SQLPaxos) checkIfExecuted(clientId int64, requestId int) (interface {}, bool) {
  op_lastSeen, ok := sp.lastSeen[clientId]
  if ok && op_lastSeen.ClientId == clientId {
     if op_lastSeen.RequestId == requestId {
        return op_lastSeen.Reply, true
     } else if op_lastSeen.RequestId > requestId {
        return nil, true // nil reply since this is an old request
     }
  }

  return nil, false
}

func (sp *SQLPaxos) reserveSlot(args interface{}, clientId int64, requestId int) int {

  // propose this operation for slot seq
  seq := sp.px.Max() + 1
  v := Op{Args: args, Reply: nil, ClientId: clientId, RequestId: requestId}
  sp.px.Start(seq, v)

  nwaits := 0
  for !sp.dead {
     decided, v_a := sp.px.Status(seq)
     if decided && v_a != nil && v_a.(Op).ClientId == v.ClientId && v_a.(Op).RequestId == v.RequestId {
        // we successfully claimed this slot for our operation
        if _, ok := sp.ops[seq]; !ok {
           sp.ops[seq] = v
        }
        break
     } else if decided {
        // another proposer got this slot, so try to get our operation in a new slot
        seq = int(math.Max(float64(sp.px.Max() + 1), float64(seq + 1)))
        sp.px.Start(seq, v)
        nwaits = 0
     } else {
        nwaits++
  	sp.mu.Unlock()
        if nwaits == 5 || nwaits == 10 {
           // re-propose our operation
           sp.px.Start(seq, v)
     	} else if nwaits > 10 {
           time.Sleep(100 * time.Millisecond)
        } else {
           time.Sleep(10 * time.Millisecond)
     	}
  	sp.mu.Lock()
     }
  }

  return seq
}

func (sp *SQLPaxos) freeMemory(seq int) {

  op_seq := sp.ops[seq]
  op_seq.Done = true
  sp.ops[seq] = op_seq
  minNotDone := seq + 1
  for i := seq; i >= 0; i-- {
     _, ok := sp.ops[i]
     if ok {
        if sp.ops[i].Done || sp.ops[i].NoOp {
           delete(sp.ops, i)
        } else {
           minNotDone = i
        }
     }
  }

  sp.px.Done(minNotDone - 1)
}

func (sp *SQLPaxos) commit(args interface {}, clientId int64, requestId int) interface {} {

  sp.mu.Lock()
  defer sp.mu.Unlock()

  // first check if this request has already been executed
  reply, ok := sp.checkIfExecuted(clientId, requestId)
  if ok {
     return reply
  }

  // reserve a slot in the paxos log for this operation
  seq := sp.reserveSlot(args, clientId, requestId)

  next := sp.next
  if next > seq {
     // our operation has already been executed
     reply = sp.ops[seq].Reply
  } else {
     // fill holes in the log and execute our operation
     reply = sp.fillHoles(next, seq)
  }

  // delete un-needed log entries to free up memory
  sp.freeMemory(seq)

  return reply
}

func (sp *SQLPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.

  // execute this operation and store the response in r
  r := sp.commit(*args, args.ClientId, args.RequestId)

  if r != nil {
     reply.Value = r.(GetReply).Value
     reply.Err = r.(GetReply).Err
  }

  return nil
}

func (sp *SQLPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.

  // execute this operation and store the response in r
  r := sp.commit(*args, args.ClientId, args.RequestId)

  if r != nil {
     reply.PreviousValue = r.(PutReply).PreviousValue
     reply.Err = r.(PutReply).Err
  }

  return nil
}

func (sp *SQLPaxos) ExecuteSQL(args *PutArgs, reply *PutReply) error {
  // Your code here.

  // execute this operation and store the response in r
  r := sp.commit(*args, args.ClientId, args.RequestId)

  if r != nil {
     reply.PreviousValue = r.(PutReply).PreviousValue
     reply.Err = r.(PutReply).Err
  }

  return nil
}

// tell the server to shut itself down.
func (sp *SQLPaxos) kill() {
  sp.dead = true
  sp.l.Close()
  sp.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *SQLPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})
  gob.Register(PutArgs{})
  gob.Register(GetArgs{})

  sp := new(SQLPaxos)
  sp.me = me

  // Your initialization code here.
  sp.ops = make(map[int]Op)
  sp.data = make(map[string]string)
  sp.lastSeen = make(map[int64]Op)
  sp.next = 0

  rpcs := rpc.NewServer()
  rpcs.Register(sp)

  sp.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sp.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sp.dead == false {
      conn, err := sp.l.Accept()
      if err == nil && sp.dead == false {
      if sp.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sp.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && sp.dead == false {
        fmt.Printf("SQLPaxos(%v) accept: %v\n", me, err.Error())
	sp.kill()
      }
    }
  }()

  return sp
}

