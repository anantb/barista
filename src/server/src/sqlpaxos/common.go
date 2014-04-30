package sqlpaxos
import "hash/fnv"
import "barista"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ConnAlreadyOpen = "ConnectionAlreadyOpen"
  ConnAlreadyClosed = "ConnectionAlreadyClosed"
)
type Err string

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
  NoOp bool
}

// for testing
const (
  Put = "Put"
  Get = "Get"
)
type ExecType string

type ExecArgs struct {
  Query string
  QueryParams [][]byte
  ClientId int64
  RequestId int
  
  // some stuff for testing
  Type ExecType
  Key string
  Value string
  DoHash bool
}

type ExecReply struct {
  Err Err
  Result *barista.ResultSet

  // some stuff for testing
  Value string
}

type OpenArgs struct {
  ClientId int64
  User string
  Password string
  Database string
  RequestId int
}

type OpenReply struct {
  Err Err
}

type CloseArgs struct {
  ClientId int64
  RequestId int
}

type CloseReply struct {
  Err Err
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

