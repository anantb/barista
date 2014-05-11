package dpaxos
/**
 * Paxos Library
 *
 * @author: David Goehring
 * @date: 5/11/2014
 */
//import "time"
import "sync"


//constants
const layout = "Jan 2, 2006 at 3:04pm (MST)"
const OK = "OK"
const REJECT = "REJECT"
type Status string

//represents a Proposal number
type PaxosProposalNum struct{
	Epoch int
	ServerName string
	ProposalNum int64
}

//contains infor pertinent to Paxos instance
type PaxosInstanceInfo struct{
  InstanceNum int
}

//represents Paxos Proposer for an instance
type PaxosProposerInstance struct{
  PaxosInstance *PaxosInstanceInfo
  MaxProposal *PaxosProposalNum
  plock sync.Mutex
}

//represents Paxos Acceptor for an instance
type PaxosAcceptorInstance struct{
  pi *PaxosInstanceInfo
  MaxProposal *PaxosProposalNum
  MaxAcceptedProposalNum *PaxosProposalNum
  MaxAcceptedProposalVal interface{}
}

//RPC arg/reply types
type PrepareArgs struct{
	InstanceNum int
	ProposalNum PaxosProposalNum
}
type PrepareReply struct{
	Epoch int
	Status Status
	MaxProposal PaxosProposalNum
	MaxProposalAccept PaxosProposalNum
	MaxProposalAcceptVal interface{}
}
type AcceptArgs struct{
	InstanceNum int
	ProposalNum PaxosProposalNum
	ProposalVal interface{}
}
type AcceptReply struct{
	Status Status
	MaxProposal PaxosProposalNum
	Leader string
	Epoch int
}
type TeachArgs struct{
	Log map[int]interface{}
	MaxDone int
	ServerName string
}
type TeachReply struct{

}

func compareProposalNums(proposal1 *PaxosProposalNum,proposal2 *PaxosProposalNum) int{
	if proposal2 == nil{
		return 1
	}
	if proposal1.Epoch > proposal2.Epoch{
		return 1
	}else{
		if proposal1.ProposalNum > proposal2.ProposalNum {
			return 1
		}else if proposal1.ProposalNum == proposal2.ProposalNum{
			if proposal1.ServerName > proposal2.ServerName{
				return 1
			}else if proposal1.ServerName == proposal2.ServerName{
				return 0
			}else{
				return -1
			}
		}else{
			return -1
		}
	}
}
func copyMap(data map[string]int) map[string]int{
	out := make(map[string]int)
	for key,val := range data{
		out[key] = val
	}
	return out
}