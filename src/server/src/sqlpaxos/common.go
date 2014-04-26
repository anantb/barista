package sqlpaxos
import "hash/fnv"
//import "barista"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ConnAlreadyOpen = "ConnectionAlreadyOpen"
  ConnAlreadyClosed = "ConnectionAlreadyClosed"
)
type Err string

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
  //Result barista.ResultSet

  // some stuff for testing
  Value string
}

type OpenArgs struct {
  ClientId int64
  Username string
  Password string
  Database string
}

type OpenReply struct {
  Err Err
}

type CloseArgs struct {
  ClientId int64
}

type CloseReply struct {
  Err Err
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

