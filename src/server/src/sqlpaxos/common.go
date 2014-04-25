package sqlpaxos
import "hash/fnv"
import "barista"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.

  ClientId int64
  RequestId int
}

type ExecArgs struct {
  Query string
  QueryParams [][]byte
  ClientId int64
  RequestId int
}

type ExecReply struct {
  Err Err
  Result barista.ResultSet
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.

  ClientId int64
  RequestId int
}

type GetReply struct {
  Err Err
  Value string
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

