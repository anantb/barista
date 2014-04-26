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
import "barista"
import "encoding/json"
import "logger"
import "db"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}

type OpType int

const (
  Open = 1
  Close = 2
  Execute = 3
  NoOp = 4
)

type Op struct {
  Type OpType // what type of operation is this: see above
  Args interface{}
  SeqNum int  
}

type LastSeen struct {
  RequestId int 
  Reply interface{}
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
  replies map[int]interface{} // the replies for this sequence number
  done map[int]bool // true if we can delete the data for this sequence number
  data map[string]string // the database
  lastSeen map[int64]LastSeen // the last request/reply for this client
  connections map[int64]db.DBManager // connections per client. Limited to a single connection per client
  next int // the next sequence number to be executed
  logger *logger.Logger // logger to write paxos log to file
}

func (sp *SQLPaxos) execute(op Op) interface{} {
  
  testing := true
  if testing {
    args := op.Args
    reply := ExecReply{}
    
    // @TODO remove this
    if op.NoOp {
       return reply
    }

    // @TODO remove get & put
    key := args.Key
    if args.Type == Put {
       // execute the put

       prevValue, ok := sp.data[key]
       if ok {
          reply.Value = prevValue
       } else {
          reply.Value = ""
       }

       if args.DoHash {
          sp.data[key] = strconv.Itoa(int(hash(reply.Value + args.Value)))
       } else {
          sp.data[key] = args.Value
       }

       reply.Err = OK

    } else if args.Type == Get {
       // execute the get

       value, ok := sp.data[key]
       if ok {
          reply.Value = value
          reply.Err = OK          
       } else {
          reply.Value = ""
          reply.Err = ErrNoKey
       }
    } 
  } else {
    // not testing

    // write op to file
    err := logger.writeToLog(op)
    if err != nil {
      // log something
    }

    switch {
    case op.OpType == Open:
      return OpenHelper(op.Args.(OpenArgs))
    case op.OpType == Close:
      return CloseHelper(op.Args.(CloseArgs))
    case op.OpType == Execute:
      return ExecuteHelper(op.Args.(ExecArgs))
    }
  }
}

func (sp *SQLPaxos) ExecuteHelper(args ExecArgs) ExecReply {
  rows, columns, err := sq.UpdateDatabase(args.ClientId, args.Query, args.Query_params)
  if err != nil {
    // log something
    return ExecReply{Result:nil, Err:err}
  }

  tuples := []*barista.Tuple{}
  for _, row := range rows {
    tuple := barista.Tuple{Cells: &row}
    tuples = append(tuples, &tuple)
  }
 
  result_set := new(barista.ResultSet)
  result_set.Con = con
  result_set.Tuples = &tuples
  result_set.FieldNames = &columns
  return ExecReply{Result:result_set, Err:nil}
}

func (sp *SQLPaxos) OpenHelper(args OpenArgs) OpenReply {
  reply := OpenReply{}
  _, ok := sp.connections[args.ClientId]
  if ok {
    reply.Err = ConnAlreadyOpen
  } else {
      manager := new(db.DBManager)
      reply.Err := manager.Connect(args.User, args.Password, args.Database)
      sp.connections[args.ClientId] = manager
  }
  _, _, err := sp.UpdateDatabase(args.ClientId, "", nil)
  if err != nil {
    // log something
  }
  return reply
}

func (sp *SQLPaxos) CloseHelper(args OpenArgs) CloseReply {
  reply := CloseReply{}
  _, ok := sp.connections[args.ClientId]
  if !ok {
    reply.Err = ConnAlreadyClosed
  } else {
    reply.Err = sp.connections[args.ClientId].CloseConnection()
    delete(sp.connections, args.ClientId) //only delete on successful close?
  }
  _, _, err := sp.UpdateDatabase(args.ClientId, "", nil)
  if err != nil {
    // log something
  }
  return reply
}

// note that NoOps don't update the state table
func (sp *Paxos) UpdateDatabase(clientId int64, query string, query_params [][]byte) 
  ([][][]byte, []string, error) {
  query := "BEGIN TRANSACTION;" + query + "; UPDATE SQLPaxosLog SET lastSeqNum=" + 
    strconv.Itoa(op.SeqNum) + "; END TRANSACTION;"
  rows, columns, err := sp.connections[clientId].ExecuteSql(query, query_params)
  return rows, columns, err
}

func (sp *SQLPaxos) fillHoles(next int, seq int) ExecReply {
 
  var reply ExecReply

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
	r, executed := sp.checkIfExecuted(sp.ops[i].Args)
        if executed {
    	   sp.replies[i] = r
	} else {
	   r := sp.execute(sp.ops[i])
	   sp.replies[i] = r
	   sp.lastSeen[sp.ops[i].Args.ClientId] = LastSeen{ RequestId: sp.ops[i].Args.RequestId, Reply: r }
	}
        sp.next++
     }

     if i == seq {
        reply = sp.replies[i]
     }
  }

  return reply
} 

// @TODO: update to support multiple types of operations
func (sp *SQLPaxos) checkIfExecuted(args interface{}) (interface{}, bool) {
  // need some casting here
  lastSeen, ok := sp.lastSeen[args.ClientId]
  if ok {
     if lastSeen.RequestId == args.RequestId {
        return lastSeen.Reply, true
     } else if lastSeen.RequestId > args.RequestId {
        return nil, true // empty reply since this is an old request
     }
  }

  return nil, false
}

func (sp *SQLPaxos) reserveSlot(args ExecArgs) int {

  // propose this operation for slot seq
  seq := sp.px.Max() + 1
  v := Op{Args: args}
  sp.px.Start(seq, v)

  nwaits := 0
  for !sp.dead {
     decided, v_a := sp.px.Status(seq)
     if decided && v_a != nil && v_a.(Op).Args.ClientId == v.Args.ClientId && v_a.(Op).Args.RequestId == v.Args.RequestId {
        // we successfully claimed this slot for our operation
        if _, ok := sp.ops[seq]; !ok {
	   v.SeqNum = seq
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
  op.SeqNum = seq // update sequence number
  return seq
}

func (sp *SQLPaxos) freeMemory(seq int) {

  sp.done[seq] = true
  minNotDone := seq + 1
  for i := seq; i >= 0; i-- {
     _, ok := sp.ops[i]
     if ok {
        if done, ok := sp.done[i]; ok && done || sp.ops[i].NoOp {
           delete(sp.ops, i)
           delete(sp.replies, i)
           delete(sp.done, i)
        } else {
           minNotDone = i
        }
     }
  }

  sp.px.Done(minNotDone - 1)
}

//@Make it work for multiple types of arguments
func (sp *SQLPaxos) commit(args interface{}) interface{} {

  sp.mu.Lock()
  defer sp.mu.Unlock()

  // first check if this request has already been executed
  reply, ok := sp.checkIfExecuted(args)
  if ok {
     return reply
  }

  // reserve a slot in the paxos log for this operation
  seq := sp.reserveSlot(args)

  next := sp.next
  if next > seq {
     // our operation has already been executed
     reply = sp.replies[seq]
  } else {
     // fill holes in the log and execute our operation
     reply = sp.fillHoles(next, seq)
  }

  // delete un-needed log entries to free up memory
  sp.freeMemory(seq)

  return reply
}

func (sp *SQLPaxos) ExecuteSQL(args *ExecArgs, reply *ExecReply) error {
  // execute this operation and store the response in r
  r := sp.commit(*args).(ExecReply)

  reply.Value = r.Value
  reply.Err = r.Err

  return nil
}

// open the connection to the database
func (sp *SQLPaxos) Open(args *OpenArgs, reply *OpenReply) error {
  // execute this operation and store the response in r
  r := sp.commit(*args).(OpenReply)

  reply.Err = r.Err
  reply.Con = r.Con
  return nil
}

// close the connection to the database
func (sp *SQLPaxos) Close(args *CloseArgs, reply *CloseReply) error {
  // execute this operation and store the response in r
  r := sp.commit(*args).(CloseReply)

  reply.Err = r.Err
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
  gob.Register(ExecArgs{})

  sp := new(SQLPaxos)
  sp.me = me

  // Your initialization code here.
  sp.ops = make(map[int]Op)
  sp.data = make(map[string]string)
  sp.replies = make(map[int]ExecReply)
  sp.done = make(map[int]bool)
  sp.lastSeen = make(map[int64]LastSeen)
  sp.next = 0
  sp.connections = make(map[int64]db.DBManager)
  sp.logger = new(logger.Logger{filename:"sqlpaxos_log.txt"})
  
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

