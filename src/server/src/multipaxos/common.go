package multipaxos

//import "time"
//import "sync"
//import "hash/fnv"
//import "strconv"
import "crypto/rand"
import "math/big"
import "net/rpc"
import "fmt"
import "time"

//constants
const(
	DEBUG = -1
	NPINGS = 5
	PINGINTERVAL = 500*time.Millisecond
	//op types
	LCHANGE = "LCHANGE"
	NORMAL = "NORMAL"
	//status codes
	OK = "OK"
	NOT_LEADER = "NOT_LEADER"
	REJECT = "REJECT"
	//Leader status
	UNKNOWN_LEADER = "UNKNOWN LEADER"
	WRONG_SERVER = "WRONG SERVER"
	INVALID_INSTANCE= "INVALID INSTANCE"
	) 
//new array type needed for sorting an array of int64
type intarr []int
func (a intarr) Len() int { return len(a) }
func (a intarr) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a intarr) Less(i, j int) bool { return a[i] < a[j] }

type Status string
type OpType string

type MultiPaxosLeader struct{
	epoch int
	id int
	numPingsMissed int
	valid bool
}
func (mpl *MultiPaxosLeader) isValid() bool{
	return !(mpl.epoch<=0 || mpl.numPingsMissed > NPINGS || !mpl.valid)
}
type MultiPaxosOP struct{
	Type OpType
	Op interface{}
}
type MultiPaxosLeaderChange struct{
	NewEpoch int
	ID int
}
type PingArgs struct{

}
type PingReply struct{
	Status Status
}
type GetInstanceDataArgs struct{
	LowestInstance int
}
type GetInstanceDataReply struct{
	Status Status
	InstancesData map[int]MultiPaxosOP
	Leader string
}
type FindLeaderArgs struct{

}
type FindLeaderReply struct{
	EpochNum int
	Leader string
}
type RemoteStartArgs struct{
	InstanceNumber int
	Op interface{}
}
type RemoteStartReply struct{
	Status Status
	Leader string
}
//***********************************************************************************************************************************//
//Helper Functions
//***********************************************************************************************************************************//

//generates random int
func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x:=bigx.Int64()
  return x
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

//debug print statement functions with priorities
func DPrint(pri int, format string) (n int, err error) {
  if DEBUG > pri {
    fmt.Printf(format)
  }
  return
}
func Log (pri int, prefix string, id string, format string) (n int, err error) {
  return DPrint(pri, "{" + prefix+" "+id+ "} " + format+"\n")
}