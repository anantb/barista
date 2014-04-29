package multipaxos

//import "time"
//import "sync"


//constants
const(
	//op types
	LCHANGE = "LCHANGE"
	NORMAL = "NORMAL"
	//status codes
	OK = "OK"
	REJECT = "REJECT"
	) 
type Status string
type OpType string

type MultiPaxosLeader struct{
	id int
	numPingsMissed int
}
type MultiPaxosOP struct{
	Type OpType
	Op interface{}
}
type MultiPaxosLeaderChange struct{
	int newEpoch
	id int
}