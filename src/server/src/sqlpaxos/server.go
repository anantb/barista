package sqlpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "reflect"
import "syscall"
import "encoding/gob"
import "math/rand"
import "strconv"
import "math"
import "barista"
import "encoding/json"
import "logger"
import "db"
import "database/sql"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
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
  connections map[int64]*db.DBManager // connections per client. Limited to a single connection per client
  next int // the next sequence number to be executed
  logger *logger.Logger // logger to write paxos log to file
}

func (sp *SQLPaxos) execute(op Op) interface{} {
  
  testing := false
  if testing {
    args := op.Args
    reply := ExecReply{}
    
    // @TODO remove this
    if op.NoOp {
       return reply
    }

    // @TODO remove get & put
    key := args.(ExecArgs).Key
    if args.(ExecArgs).Type == Put {
       // execute the put

       prevValue, ok := sp.data[key]
       if ok {
          reply.Value = prevValue
       } else {
          reply.Value = ""
       }

       if args.(ExecArgs).DoHash {
          sp.data[key] = strconv.Itoa(int(hash(reply.Value + args.(ExecArgs).Value)))
       } else {
          sp.data[key] = args.(ExecArgs).Value
       }

       reply.Err = OK

    } else if args.(ExecArgs).Type == Get {
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

    return reply

  } else {
    // not testing

    // write op to file
    err := sp.WriteToLog(op)
    if err != nil {
      fmt.Printf("Error writing operation to log\n")
    }

    switch {
    case op.Type == Open:
      return sp.OpenHelper(op.Args.(OpenArgs), op.SeqNum)
    case op.Type == Close:
      return sp.CloseHelper(op.Args.(CloseArgs), op.SeqNum)
    case op.Type == Execute:
      return sp.ExecuteHelper(op.Args.(ExecArgs), op.SeqNum)
    case op.Type == ExecuteTxn:
      return sp.ExecuteTxnHelper(op.Args.(ExecTxnArgs), op.SeqNum)
    case op.Type == BeginTxn:
      return sp.BeginTxnHelper(op.Args.(BeginTxnArgs), op.SeqNum)
    case op.Type == CommitTxn:
      return sp.CommitTxnHelper(op.Args.(CommitTxnArgs), op.SeqNum)
    case op.Type == RollbackTxn:
      return sp.RollbackTxnHelper(op.Args.(RollbackTxnArgs), op.SeqNum)
    }
  }
  return nil
}

func (sp *SQLPaxos) WriteToLog(op Op) error {
  b, err := json.Marshal(op)
  if err != nil {
    return err
  }
  return sp.logger.WriteToLog(string(b[:]))
}

func (sp *SQLPaxos) getResultSet(rows [][][]byte, columns []string) *barista.ResultSet {
  tuples := []*barista.Tuple{}
  for _, row := range rows {
    cells := make([][]byte, 0)
    vals := reflect.ValueOf(row)
    for i:=0; i < vals.Len(); i++ {
      val := vals.Index(i).Interface().([]byte)
      cells = append(cells, val)
    }
    tuple := barista.Tuple{Cells: &cells}
    tuples = append(tuples, &tuple)
  }
 
  result_set := new(barista.ResultSet)
  //result_set.Con = con. @TODO: this will not be populating this
  result_set.Tuples = &tuples
  result_set.FieldNames = &columns
  return result_set
}

func (sp *SQLPaxos) ExecuteHelper(args ExecArgs, seqnum int) ExecReply {
  rows, columns, err := sp.UpdateDatabase(args.ClientId, args.Query, args.QueryParams, seqnum)
  if err != OK {
    // log something
    return ExecReply{Err:err}
  }

  result_set := sp.getResultSet(rows, columns)
  return ExecReply{Result:result_set, Err:OK}
}

func (sp *SQLPaxos) ExecuteTxnHelper(args ExecTxnArgs, seqnum int) ExecTxnReply {

  rows := make([][][]byte, 0)
  columns := make([]string, 0)
  var err error

  if args.Query != "" {
     params := sp.convertQueryParams(args.QueryParams)
     rows, columns, err = sp.connections[args.ClientId].QueryTxn(args.Txn, args.Query, params...)

     if err != nil {
        return ExecTxnReply{Err: errorToErr(err)}
     }
  }

  sp.updateDatabaseSeqNum(args.Txn, args.ClientId, seqnum)
  
  result_set := sp.getResultSet(rows, columns)
  return ExecTxnReply{Result:result_set, Err:OK}
}

func (sp *SQLPaxos) BeginTxnHelper(args BeginTxnArgs, seqnum int) BeginTxnReply {
  tx, err := sp.connections[args.ClientId].BeginTxn()

  if err != OK {
    // log something
    return BeginTxnReply{Err:err}
  }

  sp.updateDatabaseSeqNum(tx, args.ClientId, seqnum)
  return BeginTxnReply{Txn: tx, Err:OK}
}

func (sp *SQLPaxos) CommitTxnHelper(args CommitTxnArgs, seqnum int) CommitTxnReply {

  sp.updateDatabaseSeqNum(args.Txn, args.ClientId, seqnum)

  err := sp.connections[args.ClientId].CommitTxn(args.Txn)
  if err != nil {
     return CommitTxnReply{Err: errorToErr(err)}
  }

  return CommitTxnReply{Err:OK}
}

func (sp *SQLPaxos) RollbackTxnHelper(args RollbackTxnArgs, seqnum int) RollbackTxnReply {

  err := sp.connections[args.ClientId].RollbackTxn(args.Txn)
  if err != nil {
     return RollbackTxnReply{Err: errorToErr(err)}
  }

  _, _, err = sp.UpdateDatabase(args.ClientId, "", nil, seqnum)
  if err != OK {
    // log something
  }

  return RollbackTxnReply{Err:OK}
}

func (sp *SQLPaxos) OpenHelper(args OpenArgs, seqnum int) OpenReply {
  reply := OpenReply{}
  _, ok := sp.connections[args.ClientId]
  if ok {
    reply.Err = ConnAlreadyOpen
  } else {
      manager := new(db.DBManager)
      reply.Err = errorToErr(manager.OpenConnection(args.User, args.Password, args.Database))
      sp.connections[args.ClientId] = manager
  }
  _, _, err := sp.UpdateDatabase(args.ClientId, "", nil, seqnum)
  if err != OK {
    // log something
  }
  return reply
}

func errorToErr(error error) Err {
  if error != nil {
    return Err(error.Error())
  } else {
    return OK
  }
}

func (sp *SQLPaxos) CloseHelper(args CloseArgs, seqnum int) CloseReply {
  _, _, err := sp.UpdateDatabase(args.ClientId, "", nil, seqnum)
  reply := CloseReply{}
  _, ok := sp.connections[args.ClientId]
  if !ok {
    reply.Err = ConnAlreadyClosed
  } else {
    reply.Err = errorToErr(sp.connections[args.ClientId].CloseConnection())
    delete(sp.connections, args.ClientId) //only delete on successful close?
  }
  if err != OK {
    // log something
  }
  return reply
}

func (sp *SQLPaxos) convertQueryParams(query_params [][]byte) []interface{} {
  params := make([]interface{}, len(query_params))
  for i, param := range query_params {
     params[i] = param
  }
  return params
}


// note that NoOps don't update the state table
func (sp *SQLPaxos) UpdateDatabase(clientId int64, query string, query_params [][]byte, seqnum int) ([][][]byte, []string, Err) {
  tx, err := sp.connections[clientId].BeginTxn()
  
  rows := make([][][]byte, 0)
  columns := make([]string, 0)

  if err != nil || tx == nil {
     return rows, columns, errorToErr(err)
  }

  if query != "" {
     params := sp.convertQueryParams(query_params)
     rows, columns, err = sp.connections[clientId].QueryTxn(tx, query, params...)
  }

  sp.updateDatabaseSeqNum(tx, clientId, seqnum)

  errCommit := sp.connections[clientId].CommitTxn(tx)
  if errCommit != nil {
     fmt.Println("Error committing txn: ", errCommit)
  }

  return rows, columns, errorToErr(err)
}

func (sp *SQLPaxos) updateDatabaseSeqNum(tx *sql.Tx, clientId int64, seqnum int) {
  update := "UPDATE sqlpaxoslog SET lastseqnum=" + strconv.Itoa(seqnum) + ";"
  params := make([]interface{}, 0)
  _, err := sp.connections[clientId].ExecTxn(tx, update, params...)
  if err != nil {
    fmt.Println("Error updating SQLPaxosLog: ", err)
  }
}

func (sp *SQLPaxos) fillHoles(next int, seq int) interface{} {
 
  var reply interface{}

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
           v := v_i.(Op)
           v.SeqNum = i
           sp.ops[i] = v
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
      r, executed := sp.checkIfExecuted(sp.ops[i])
            if executed {
             sp.replies[i] = r
      } else {
         r := sp.execute(sp.ops[i])
         sp.replies[i] = r
         sp.lastSeen[getOpClientId(sp.ops[i])] = LastSeen{ RequestId: getOpRequestId(sp.ops[i]), Reply: r }
      }
        sp.next++
     }

     if i == seq {
        reply = sp.replies[i]
     }
  }

  return reply
} 

func getOpClientId(op Op) int64 {
  switch {
  case op.Type == Open:
    return op.Args.(OpenArgs).ClientId;
  case op.Type == Close:
    return op.Args.(CloseArgs).ClientId;
  case op.Type == Execute:
    return op.Args.(ExecArgs).ClientId;
  case op.Type == ExecuteTxn:
    return op.Args.(ExecTxnArgs).ClientId;
  case op.Type == BeginTxn:
    return op.Args.(BeginTxnArgs).ClientId;
  case op.Type == CommitTxn:
    return op.Args.(CommitTxnArgs).ClientId;
  case op.Type == RollbackTxn:
    return op.Args.(RollbackTxnArgs).ClientId;
  }
  return -1;
}

func getOpRequestId(op Op) int {
  switch {
  case op.Type == Open:
    return op.Args.(OpenArgs).RequestId;
  case op.Type == Close:
    return op.Args.(CloseArgs).RequestId;
  case op.Type == Execute:
    return op.Args.(ExecArgs).RequestId;
  case op.Type == ExecuteTxn:
    return op.Args.(ExecTxnArgs).RequestId;
  case op.Type == BeginTxn:
    return op.Args.(BeginTxnArgs).RequestId;
  case op.Type == CommitTxn:
    return op.Args.(CommitTxnArgs).RequestId;
  case op.Type == RollbackTxn:
    return op.Args.(RollbackTxnArgs).RequestId;
  }
  return -1;
}

// @TODO: update to support multiple types of operations
func (sp *SQLPaxos) checkIfExecuted(op Op) (interface{}, bool) {
  // need some casting here
  lastSeen, ok := sp.lastSeen[getOpClientId(op)]
  if ok {
     if lastSeen.RequestId == getOpRequestId(op) {
        return lastSeen.Reply, true
     } else if lastSeen.RequestId > getOpRequestId(op) {
        return nil, true // empty reply since this is an old request
     }
  }

  return nil, false
}

func (sp *SQLPaxos) reserveSlot(op Op) int {

  // propose this operation for slot seq
  seq := sp.px.Max() + 1
  v := op
  sp.px.Start(seq, v)

  nwaits := 0
  for !sp.dead {
     decided, v_a := sp.px.Status(seq)
     // @TODO: need special case for NoOp
     if decided && ((v.NoOp && v_a != nil && v_a.(Op).NoOp) || 
      (v_a != nil && getOpClientId(v_a.(Op)) == getOpClientId(v) && 
       getOpRequestId(v_a.(Op)) == getOpRequestId(v))) {
        // we successfully claimed this slot for our operation
        if _, ok := sp.ops[seq]; !ok {
           fmt.Printf("achieved consensus for seqnum: %d\n", seq)
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
func (sp *SQLPaxos) commit(op Op) interface{} {

  sp.mu.Lock()
  defer sp.mu.Unlock()

  // first check if this request has already been executed
  reply, ok := sp.checkIfExecuted(op)
  if ok {
     return reply
  }

  // reserve a slot in the paxos log for this operation
  seq := sp.reserveSlot(op)

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
  op := Op{Type:Execute, Args: *args}
  r := sp.commit(op)

  if r != nil {
     reply.Result = r.(ExecReply).Result
     reply.Value = r.(ExecReply).Value
     reply.Err = r.(ExecReply).Err
  }

  return nil
}

func (sp *SQLPaxos) ExecuteSQLTxn(args *ExecTxnArgs, reply *ExecTxnReply) error {
  // execute this operation and store the response in r
  op := Op{Type:ExecuteTxn, Args: *args}
  r := sp.commit(op)

  if r != nil {
     reply.Result = r.(ExecTxnReply).Result
     reply.Err = r.(ExecTxnReply).Err
  }

  return nil
}

func (sp *SQLPaxos) BeginTxn(args *BeginTxnArgs, reply *BeginTxnReply) error {
  // execute this operation and store the response in r
  op := Op{Type:BeginTxn, Args: *args}
  r := sp.commit(op)

  if r != nil {
     reply.Err = r.(BeginTxnReply).Err
     reply.Txn = r.(BeginTxnReply).Txn
  }

  return nil
}

func (sp *SQLPaxos) CommitTxn(args *CommitTxnArgs, reply *CommitTxnReply) error {
  // execute this operation and store the response in r
  op := Op{Type:CommitTxn, Args: *args}
  r := sp.commit(op)

  if r != nil {
     reply.Err = r.(CommitTxnReply).Err
  }

  return nil
}

func (sp *SQLPaxos) RollbackTxn(args *RollbackTxnArgs, reply *RollbackTxnReply) error {
  // execute this operation and store the response in r
  op := Op{Type:RollbackTxn, Args: *args}
  r := sp.commit(op)

  if r != nil {
     reply.Err = r.(RollbackTxnReply).Err
  }

  return nil
}

// open the connection to the database
func (sp *SQLPaxos) Open(args *OpenArgs, reply *OpenReply) error {
  // execute this operation and store the response in r
  op := Op{Type:Open, Args: *args}
  r := sp.commit(op)

  if r != nil {
     reply.Err = r.(OpenReply).Err
  }

  return nil
}

// close the connection to the database
func (sp *SQLPaxos) Close(args *CloseArgs, reply *CloseReply) error {
  // execute this operation and store the response in r
  op := Op{Type:Close, Args: *args}
  r := sp.commit(op)

  if r != nil {
     reply.Err = r.(CloseReply).Err
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
  gob.Register(ExecArgs{})
  gob.Register(ExecTxnArgs{})
  gob.Register(BeginTxnArgs{})
  gob.Register(CommitTxnArgs{})
  gob.Register(RollbackTxnArgs{})
  gob.Register(OpenArgs{})
  gob.Register(CloseArgs{})

  sp := new(SQLPaxos)
  sp.me = me

  // Your initialization code here.
  sp.ops = make(map[int]Op)
  sp.data = make(map[string]string)
  sp.replies = make(map[int]interface{})
  sp.done = make(map[int]bool)
  sp.lastSeen = make(map[int64]LastSeen)
  sp.next = 0
  sp.connections = make(map[int64]*db.DBManager)
  sp.logger = logger.Make("sqlpaxos_log.txt")
  
  rpcs := rpc.NewServer()
  rpcs.Register(sp)

  sp.px = paxos.Make(servers, me, rpcs)

  //os.Remove(servers[me]) // only needed for "unix"
  //l, e := net.Listen("unix", servers[me]);
  l, e := net.Listen("tcp", servers[me] + paxos.PORT);
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
          //c1 := conn.(*net.UnixConn)
          c1 := conn.(*net.TCPConn)
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

