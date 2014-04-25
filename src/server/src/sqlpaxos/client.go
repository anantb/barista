package sqlpaxos

import "net/rpc"
import "fmt"
import "sync"
import crand "crypto/rand"
import "math/big"

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := crand.Int(crand.Reader, max)
  x := bigx.Int64()
  return x
}

type Clerk struct {
  servers []string
  mu sync.Mutex
  me int64 // passed as clientId
  curRequest int
}


func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers

  ck.me = nrand()
  ck.curRequest = 0

  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  ck.curRequest++

  // try each server 
  for _, srv := range ck.servers {
     args := &GetArgs{}
     args.Key = key
     args.ClientId = ck.me
     args.RequestId = ck.curRequest
     var reply GetReply
     ok := call(srv, "SQLPaxos.Get", args, &reply)
     if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
        return reply.Value
     }
  }

  return ""
}

func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  // You'll have to modify Put().
  ck.curRequest++

  for _, srv := range ck.servers {
     args := &PutArgs{}
     args.Key = key
     args.Value = value
     args.DoHash = dohash
     args.ClientId = ck.me
     args.RequestId = ck.curRequest
     var reply PutReply
     ok := call(srv, "SQLPaxos.Put", args, &reply)
     if ok && reply.Err == OK {
        return reply.PreviousValue
     }
  }

  return ""
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
