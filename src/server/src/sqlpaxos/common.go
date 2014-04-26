package sqlpaxos
import "hash/fnv"
//import "barista"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
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
  //Con barista.Connection

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

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

