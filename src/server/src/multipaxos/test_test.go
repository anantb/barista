package multipaxos

import "testing"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"
//import "log"
import "math/rand"
//import "sort"

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "px-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}
func getandcheckleader(t *testing.T, pxa []*MultiPaxos) int{
  out:=-1
  epoch:=0
  to := 10 * time.Millisecond
  for iters := 0; iters < 50; iters++ {
    for i:=0; i<len(pxa); i++{
      current := pxa[i]
      if current!=nil && current.isLeaderAndValid(){
        if(out!=-1 && current.leader.epoch == epoch){
          t.Fatalf("Found too many leaders!")
        }
        if current.leader.epoch > epoch{
          out = i
          epoch = current.leader.epoch
        }
      }
    }
    if(out!=-1){
      break
    }
    time.Sleep(to)
    if to < time.Second {
      to *= 2
    }
  }
  if out == -1{
    t.Fatalf("No leader has been elected!")
  }
  return out
}
func checkval(t *testing.T, pxa []*MultiPaxos, seq int, expected interface{}){
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      decided, v1 := pxa[i].Status(seq)
      //need to ignore the nils here because that indicates the client op didn't succeed
      //and that slot was filled by the leader failover op
      if decided && v1 != expected && v1!=nil{
            t.Fatalf("decided values do not match; seq=%v i=%v me=%v expected=%v actual=%v",
            seq, i, pxa[i].me, expected, v1)
      }
    }
  }
}
func ndecided(t *testing.T, pxa []*MultiPaxos, seq int) int {
  count := 0
  var v interface{}
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      decided, v1 := pxa[i].Status(seq)
      if decided {
        if count > 0 && v != v1 {
          t.Fatalf("decided values do not match; seq=%v i=%v me=%v v=%v v1=%v",
            seq, i, pxa[i].me, v, v1)
        }
        count++
        v = v1
      }
    }
  }
  return count
}

func waitn(t *testing.T, pxa[]*MultiPaxos, seq int, wanted int) {
  to := 10 * time.Millisecond
  for iters := 0; iters < 50; iters++ {
    if ndecided(t, pxa, seq) >= wanted {
      break
    }
    time.Sleep(to)
    if to < time.Second {
      to *= 2
    }
  }
  nd := ndecided(t, pxa, seq)
  if nd < wanted {
    getandcheckleader(t,pxa)
    t.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
  }
}

func waitmajority(t *testing.T, pxa[]*MultiPaxos, seq int) {
  waitn(t, pxa, seq, (len(pxa) / 2) + 1)
}

func checkmax(t *testing.T, pxa[]*MultiPaxos, seq int, max int) {
  time.Sleep(3 * time.Second)
  nd := ndecided(t, pxa, seq)
  if nd > max {
    t.Fatalf("too many decided; seq=%v ndecided=%v max=%v", seq, nd, max)
  }
}

func cleanup(pxa []*MultiPaxos) {
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      pxa[i].Kill()
    }
  }
}

func noTestSpeed(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const nMultiPaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)

  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("time", i)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  t0 := time.Now()

  for i := 0; i < 20; i++ {
    pxa[0].Start(i, "x")
    waitn(t, pxa, i, nMultiPaxos)
  }

  d := time.Since(t0)
  fmt.Printf("20 agreements %v seconds\n", d.Seconds())
}
func execute(pxa []*MultiPaxos,i int,seq int, v interface{}){
  ok := false
  for !ok && !pxa[i].dead{
    ok,_ = pxa[i].Status(seq)
    pxa[i].Start(seq, v)
    time.Sleep(time.Duration(100 + rand.Int63() % 500) * time.Millisecond)
  }
}
func TestBasic(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const nMultiPaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)

  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("basic", i)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Single proposer ...\n")

  go execute(pxa,0,0,"hello")

  waitn(t, pxa, 0, nMultiPaxos)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Many proposers, same value ...\n")

  for i := 0; i < nMultiPaxos; i++ {
    go execute(pxa,i,1,77)
  }
  waitn(t, pxa, 1, nMultiPaxos)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Many proposers, different values ...\n")

  go execute(pxa,0,2,100)
  go execute(pxa,1,2,101)
  go execute(pxa,2,2,102)
  waitn(t, pxa, 2, nMultiPaxos)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Out-of-order instances ...\n")

  go execute(pxa,0,7,700)
  go execute(pxa,0,6,600)
  go execute(pxa,1,5,500)
  go execute(pxa,0,4,400)
  go execute(pxa,1,3,300)
  waitn(t, pxa, 7, nMultiPaxos)
  waitn(t, pxa, 6, nMultiPaxos)
  waitn(t, pxa, 5, nMultiPaxos)
  waitn(t, pxa, 4, nMultiPaxos)
  waitn(t, pxa, 3, nMultiPaxos)

  if pxa[0].Max() != 8 {
    t.Fatalf("wrong Max()")
  }

  fmt.Printf("  ... Passed\n")
}
func TestDeaf(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const nMultiPaxos = 5
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)

  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("deaf", i)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Deaf proposer should still get data ...\n")

  //deaf proposer should still get data as a result of the ping pggy back where instance data is sent in the 
  //reply to the proposer

  go execute(pxa,0,0,"hello")
  waitn(t, pxa, 0, nMultiPaxos)

  l := getandcheckleader(t,pxa)

  firstDeaf := (l+2)%len(pxh)
  secondDeaf := (l+1)%len(pxh)
  os.Remove(pxh[firstDeaf])
  os.Remove(pxh[secondDeaf])

  go execute(pxa,l,1,"goodbye")
  waitmajority(t, pxa, 1)
  time.Sleep(1 * time.Second)
  if ndecided(t, pxa, 1) != nMultiPaxos{
    t.Fatalf("a deaf peer should have heard about a decision")
  }

  go execute(pxa,firstDeaf,1,"xxx")
  waitn(t, pxa, 1, nMultiPaxos-1)
  time.Sleep(1 * time.Second)
  if ndecided(t, pxa, 1) != nMultiPaxos {
    t.Fatalf("a deaf peer should heard about a decision")
  }

  go execute(pxa,secondDeaf,1,"yyy")
  waitn(t, pxa, 1, nMultiPaxos)

  fmt.Printf("  ... Passed\n")
}
func TestBasicLeaderModified(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const nMultiPaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)

  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("basicLeaderModified", i)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  fmt.Printf("Test: Basic LeaderModified Elect Leader Instance -1...\n")

  waitn(t, pxa, -1,nMultiPaxos)

  l := getandcheckleader(t,pxa)
  
  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Basic LeaderModified  Single Agreement to leader...\n")
  go execute(pxa,l,0,"hello1")

  waitn(t, pxa, 0,nMultiPaxos)
  checkval(t,pxa,0,"hello1")

  go execute(pxa,l,1,"hello")

  waitn(t, pxa, 1,nMultiPaxos)
  checkval(t,pxa,1,"hello")

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Basic LeaderModified Many proposers, same value...\n")

  for i := 0; i < nMultiPaxos; i++ {
    go execute(pxa,i,2,77)
  }
  waitn(t, pxa, 2,nMultiPaxos)

  checkval(t,pxa,2,77)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Basic LeaderModified Many proposers, different values...\n")

  data := [3]int{100,101,102}

  go execute(pxa,0,3,data[0])
  go execute(pxa,1,3,data[1])
  go execute(pxa,2,3,data[2])

  waitn(t, pxa, 3,nMultiPaxos)

  //checkval(t,pxa,3,500)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Basic LeaderModified Out-of-order instances...\n")

  go execute(pxa,l,8,300)
  go execute(pxa,l,7,600)
  go execute(pxa,l,6,500)
  go execute(pxa,l,5,400)
  go execute(pxa,l,4,300)

  //waiting for instance 8 to complete means all the rest before it completed
  //can't respong to client for instance i until the results of all previous instances known
  waitn(t, pxa, 8,nMultiPaxos)
  checkval(t,pxa,4,300)
  checkval(t,pxa,5,400)
  checkval(t,pxa,6,500)
  checkval(t,pxa,7,600)
  checkval(t,pxa,8,300)
  
  fmt.Printf("  ... Passed\n")
}
func TestRPCCount(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: RPC counts aren't too high ...\n")

  const nMultiPaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)

  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("count", i)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }

  waitn(t, pxa, -1,nMultiPaxos)

  l := getandcheckleader(t,pxa)

  time.Sleep(PINGWAIT)
  //initial total after initial leader election
  totalbase := 0
  for j := 0; j < nMultiPaxos; j++ {
    totalbase += pxa[j].GetRPCCount()
  }

  ninst1 := 5
  seq := 0
  for i := 0; i < ninst1; i++ {
    go execute(pxa,l,seq,"x")
    //pxa[l].Start(seq, "x")
    waitn(t, pxa, seq, nMultiPaxos)
    checkval(t,pxa,seq,"x")
    seq++
  }

  time.Sleep(PINGWAIT)
  
  total1 := 0
  for j := 0; j < nMultiPaxos; j++ {
    total1 += pxa[j].GetRPCCount()
  }

  totalactual := total1 - totalbase
  // per agreement:
  // 3 accepts
  // 3 decides
  // ninst1 agreements
  expected1 := ninst1 * (nMultiPaxos + nMultiPaxos)
  if totalactual > expected1 {
    t.Fatalf("too many RPCs for serial Start()s; %v instances, got %v, expected %v",
      ninst1, total1, expected1)
  }
  fmt.Printf(" Total messages used: "+strconv.Itoa(totalactual)+" ... Passed\n")
  
  ninst2 := 5
  for i := 0; i < ninst2; i++ {
    for j := 0; j < nMultiPaxos; j++ {
      go execute(pxa,j,seq, j + (i * 10))
    }
    waitn(t, pxa, seq, nMultiPaxos)
    seq++
  }

  time.Sleep(4 * time.Second)

  total2 := 0
  for j := 0; j < nMultiPaxos; j++ {
    total2 += pxa[j].GetRPCCount()
  }

  //fmt.Printf(" Total messages used Actual: "+strconv.Itoa(total2)+"\n")

  total2 -= total1

  // normal worst case per agreement (with contention):
  // Proposer 1: 3 prep, 3 acc, 3 decides.
  // Proposer 2: 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
  // Proposer 3: 3 prep, 3 acc, 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
  //using multipaxos it should be like normal case with no contention
  expected2 := ninst2 * (nMultiPaxos + nMultiPaxos)
  if total2 > expected2 {
    t.Fatalf("too many RPCs for concurrent Start()s; %v instances, got %v, expected %v",
      ninst2, total2, expected2)
  }

  fmt.Printf(" Total messages used: "+strconv.Itoa(total2)+"  ... Passed\n")
}

//
// many agreements (without failures)
//
func TestMany(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many instances ...\n")

  const nMultiPaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)

  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("many", i)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }
  waitn(t, pxa, -1,nMultiPaxos)

  getandcheckleader(t,pxa)

  const ninst = 50
  for seq := 0; seq < ninst; seq++ {
    for i := 0; i < nMultiPaxos; i++ {
      pxa[i].Start(seq, (seq * 10) + i)
    }
  }

  for {
    done := true
    for seq := 0; seq < ninst; seq++ {
      if ndecided(t, pxa, seq) < nMultiPaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }

  fmt.Printf("  ... Passed\n")
}
func TestLeaderDeaths(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const nMultiPaxos = 5
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)

  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("leaderdeaths", i)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }
  fmt.Printf("Test: First leader death, make sure changes ...\n")

  waitmajority(t, pxa, -1)
  //t.Fatalf("original=%v",pxa)

  l := getandcheckleader(t,pxa)
    
  //make a few agreements
  go execute(pxa,l,0,"lol0")
  go execute(pxa,l,1,"lol1")
  go execute(pxa,l,2,"lol2")
  go execute(pxa,l,3,"lol3")
  go execute(pxa,l,4,"lol4")

  getandcheckleader(t,pxa)

  waitn(t, pxa, 4,nMultiPaxos)
  checkval(t,pxa,1,"lol1")
  checkval(t,pxa,2,"lol2")
  checkval(t,pxa,3,"lol3")
  checkval(t,pxa,4,"lol4")

  startTime := time.Now()

  pxa[l].Kill()

  //wait for replicas to detect failure
  time.Sleep(PINGWAIT)

  //fmt.Printf("old=%v >>>>>>>>>>>>>>>>>>>>>>>>",pxa)
  pxaNewSize := len(pxa)-1
  pxanew := make([]*MultiPaxos,0,pxaNewSize)
  pxanew =append(pxanew, pxa[:l]...)
  pxanew =append(pxanew, pxa[l+1:]...)
  //fmt.Printf("new=%v >>>>>>>>>>>>>>>>>>>>>>>>>>",pxanew)
  lnew := getandcheckleader(t,pxanew)

  endTime := time.Now()
  fmt.Printf("Leader 1 failover took: "+strconv.Itoa(int((endTime.Sub(startTime)).Seconds()))+" seconds \n")

  //t.Fatalf("lnew=%v l=%v old=%v new=%v",lnew,l,pxa,pxanew)
  if pxa[l].me == pxanew[lnew].me{
    t.Fatalf("Leader did not change despite having failed!")
  }
  maxAfterFail := pxanew[lnew].Max()

  go execute(pxa,lnew,maxAfterFail,"lol5")
  waitn(t, pxanew, maxAfterFail, nMultiPaxos-1)

  checkval(t,pxanew,1,"lol1")
  checkval(t,pxanew,2,"lol2")
  checkval(t,pxanew,3,"lol3")
  checkval(t,pxanew,4,"lol4")
  checkval(t,pxanew,maxAfterFail,"lol5")

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Second leader death and in flight requests...\n")

  nRequests := 20
  cutoff := nRequests/2 + 1

  for j:=1; j<nRequests+1; j++{
    pxanew[lnew].Start(maxAfterFail+j,"before"+strconv.Itoa(j))
    if(j==cutoff){
      //give some propagation time 
      time.Sleep(10*time.Millisecond)
      pxanew[lnew].Kill()
    }
  }
  startTime2:=time.Now()
  //wait for replicas to detect failure
  time.Sleep(PINGWAIT)

  pxaNew1Size := len(pxanew)-1
  pxanew1 := make([]*MultiPaxos,0,pxaNew1Size)
  pxanew1 =append(pxanew1, pxanew[:lnew]...)
  pxanew1 =append(pxanew1, pxanew[lnew+1:]...)

  lnew1 := getandcheckleader(t,pxanew1)
  endTime2:=time.Now()
  fmt.Printf("Leader 2 failover took: "+strconv.Itoa(int((endTime2.Sub(startTime2)).Seconds()))+" seconds \n")
  //t.Fatalf("lnew=%v lnew1=%v old=%v new=%v",lnew,lnew1,pxanew,pxanew1)

  if pxanew1[lnew1].me == pxanew[lnew].me || pxanew1[lnew1].me == pxa[l].me{
      t.Fatalf("Leader did not change despite having failed!; olderleader=%v oldleader=%v newleader=%v",
            pxa[l].me, pxanew[lnew].me , pxanew1[lnew1].me)
  }
  fmt.Printf("Second Leader Death Failover Passed...\n")

  //find leader failover location
  failOverLocation := -1
  for j:=1; j<nRequests+1; j++{
    ok,val := pxanew1[lnew1].Status(maxAfterFail+j)
    if ok{
      if val == nil{
        failOverLocation = maxAfterFail+j
        break
      }
    }
  }
  if failOverLocation == -1{
    t.Fatalf("Leader failover op not detected!")
  }
  missed := false
  for j:=1; j<nRequests+1; j++{
    //even if old leader sent agreement data and it was agreed on after the leader change,
    //none of that data should be displayed to the user of the MP API
    if maxAfterFail+j > failOverLocation{
      for _,pi := range pxanew1{
        okpi,val := pi.Status(maxAfterFail+j)
        if okpi != false{
          t.Fatalf("Paxos replica returned a value it shouldn't have %v at %v when failover occurred at:%v! It returned a value that has become invalid due to the failover!",val,maxAfterFail+j,failOverLocation)
        }
      }
      //if didn't finish then have new leader submit new value, should win
      go execute(pxanew1,lnew1,maxAfterFail+j,"after"+strconv.Itoa(j))
      waitmajority(t, pxanew1, maxAfterFail+j)
      checkval(t,pxanew1,maxAfterFail+j,"after"+strconv.Itoa(j))
    }
    //check to see that everything old leader said happened actually stayed the same
    okOld,_ := pxanew[lnew].Status(maxAfterFail+j)
    if !okOld{
      missed = true
    }else{
      checkval(t,pxanew1,maxAfterFail+j,"before"+strconv.Itoa(j))
      if missed == true{
        t.Fatalf("Old leader returned a value when it should not have! There was a hole in the paxos log before the return.")
      }
    }
  } 
  fmt.Printf("Second Leader Death In Flight Request Override/Recovery Passed...\n")
  fmt.Printf("  ... Passed\n")
}
func TestFallBehind(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Paxos Replica Falls Behind and then Catches Up With Data From Leader...\n")

  const nMultiPaxos = 5
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)

  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("old", i)
  }

  pxa[1] = Make(pxh, 1, nil)
  pxa[2] = Make(pxh, 2, nil)
  pxa[3] = Make(pxh, 3, nil)

  waitmajority(t, pxa, -1)

  l := getandcheckleader(t,pxa)

  nRequests := 20
  for seq:=0; seq<nRequests; seq++{
    go execute(pxa,l,seq,100+seq)
  }
  waitmajority(t, pxa, 19)

  pxa[0] = Make(pxh, 0, nil)

  //wait for paxos 0 to catch up
  waitn(t, pxa, 19, 4)

  for seq:=0; seq<nRequests; seq++{
    r0ok, r0val := pxa[0].Status(seq)
    _, rlval := pxa[l].Status(seq)
    if !r0ok || r0val != rlval{
      t.Fatalf("Delayed replica did not get all the necessary values!")
    }
    checkval(t,pxa,seq,rlval)
  }

  fmt.Printf("  ... Passed\n")
}


func TestForget(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const nMultiPaxos = 6
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)
  
  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("gc", i)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }
  waitn(t, pxa, -1, nMultiPaxos)

  fmt.Printf("Test: Forgetting ...\n")

  // initial Min() correct?
  for i := 0; i < nMultiPaxos; i++ {
    m := pxa[i].Min()
    //because -1 is used for initial leader agreement
    if m > 1 {
      t.Fatalf("wrong initial Min() %v", m)
    }
  }

  pxa[0].Start(0, "00")
  pxa[1].Start(1, "11")
  pxa[2].Start(2, "22")
  pxa[0].Start(3, "00")
  pxa[1].Start(4, "11")
  pxa[2].Start(5, "22")
  pxa[0].Start(6, "66")
  pxa[1].Start(7, "77")

  waitn(t, pxa, 0, nMultiPaxos)

  time.Sleep(nMultiPaxos*PINGINTERVAL)

  // Min() correct?
  for i := 0; i < nMultiPaxos; i++ {
    m := pxa[i].Min()
    if m != 1 {
      t.Fatalf("wrong Min() %v; expected 1", m)
    }
  }

  waitn(t, pxa, 1, nMultiPaxos)

  time.Sleep(nMultiPaxos*PINGINTERVAL)
  // Min() correct?
  for i := 0; i < nMultiPaxos; i++ {
    m := pxa[i].Min()
    if m != 1 {
      t.Fatalf("wrong Min() %v; expected 0", m)
    }
  }

  // everyone Done() -> Min() changes?
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i].Done(0)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i].Done(1)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i].Start(8 + i, "xx")
  }

  allok := false
  for iters := 0; iters < 12; iters++ {
    allok = true
    for i := 0; i < nMultiPaxos; i++ {
      s := pxa[i].Min()
      if s != 2 {
        allok = false
      }
    }
    if allok {
      break
    }
    time.Sleep(nMultiPaxos*PINGINTERVAL)
  }
  if allok != true {
    t.Fatalf("Min() did not advance after Done()")
  }

  fmt.Printf("  ... Passed\n")
}

func TestManyForget(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const nMultiPaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)
  
  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("manygc", i)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
    pxa[i].SetUnreliable(true)
  }

  fmt.Printf("Test: Lots of forgetting ...\n")

  const maxseq = 20
  done := false

  go func() {
    na := rand.Perm(maxseq)
    for i := 0; i < len(na); i++ {
      seq := na[i]
      j := (rand.Int() % nMultiPaxos)
      v := rand.Int() 
      go execute(pxa,j,seq,v)
      //pxa[j].Start(seq, v)
      runtime.Gosched()
    }
  }()

  go func() {
    for done == false {
      seq := (rand.Int() % maxseq)
      i := (rand.Int() % nMultiPaxos)
      if seq >= pxa[i].Min() {
        decided, _ := pxa[i].Status(seq)
        if decided {
          pxa[i].Done(seq)
        }
      }
      runtime.Gosched()
    }
  }()

  time.Sleep(5 * time.Second)
  done = true
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i].SetUnreliable(false)
  }
  time.Sleep(2 * time.Second)

  for seq := 0; seq < maxseq; seq++ {
    for i := 0; i < nMultiPaxos; i++ {
      if seq >= pxa[i].Min() {
        pxa[i].Status(seq)
      }
    }
  }

  fmt.Printf("  ... Passed\n")
}

//
// does MultiPaxos forgetting actually free the memory?
//
func TestForgetMem(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: MultiPaxos frees forgotten instance memory ...\n")

  const nMultiPaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)
  
  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("gcmem", i)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
  }
  waitn(t, pxa,-1,nMultiPaxos)

  go execute(pxa,0,0,"x")
  waitn(t, pxa, 0, nMultiPaxos)

  runtime.GC()
  var m0 runtime.MemStats
  runtime.ReadMemStats(&m0)
  // m0.Alloc about a megabyte

  for i := 1; i <= 10; i++ {
    big := make([]byte, 1000000)
    for j := 0; j < len(big); j++ {
      big[j] = byte('a' + rand.Int() % 26)
    }
    go execute(pxa,0,i,string(big))
    //pxa[0].Start(i, string(big))
    waitn(t, pxa, i, nMultiPaxos)
  }

  runtime.GC()
  var m1 runtime.MemStats
  runtime.ReadMemStats(&m1)
  // m1.Alloc about 90 megabytes

  for i := 0; i < nMultiPaxos; i++ {
    pxa[i].Done(10)
  }
  for i := 0; i < nMultiPaxos; i++ {
    go execute(pxa,i,11+i,"z")
  }
  time.Sleep(3 * time.Second)
  for i := 0; i < nMultiPaxos; i++ {
    if pxa[i].Min() != 11 {
      t.Fatalf("expected Min() %v, got %v\n", 11, pxa[i].Min())
    }
  }

  time.Sleep(nMultiPaxos*PINGINTERVAL)

  runtime.GC()
  var m2 runtime.MemStats
  runtime.ReadMemStats(&m2)
  // m2.Alloc about 10 megabytes

  if m2.Alloc > (m1.Alloc / 2) {
    t.Fatalf("memory use did not shrink enough")
  }

  fmt.Printf("  ... Passed\n")
}


//
// a peer starts up, with proposal, after others decide.
// then another peer starts, without a proposal.
// 
func TestOld(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Minority proposal ignored and slow instance catches up by talking to leader...\n")

  const nMultiPaxos = 5
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)

  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("old", i)
  }

  pxa[1] = Make(pxh, 1, nil)
  pxa[2] = Make(pxh, 2, nil)
  pxa[3] = Make(pxh, 3, nil)

  waitmajority(t, pxa, -1)

  l := getandcheckleader(t,pxa)

  go execute(pxa,l,0,111)
  go execute(pxa,l,1,111)

  waitmajority(t, pxa, 1)

  pxa[0] = Make(pxh, 0, nil)
  go execute(pxa,0,1,222)

  waitn(t, pxa, 1, 4)

  if false {
    pxa[4] = Make(pxh, 4, nil)
    waitn(t, pxa, 1, nMultiPaxos)
  }

  fmt.Printf("  ... Passed\n")
}

//
// many agreements, with unreliable RPC
//
func TestManyUnreliable(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many instances, unreliable RPC ...\n")

  const nMultiPaxos = 3
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  var pxh []string = make([]string, nMultiPaxos)
  defer cleanup(pxa)

  for i := 0; i < nMultiPaxos; i++ {
    pxh[i] = port("manyun", i)
  }
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i] = Make(pxh, i, nil)
    pxa[i].SetUnreliable(true)
    go execute(pxa,i,0,0)
  }

  const ninst = 50
  for seq := 1; seq < ninst; seq++ {
    // only 3 active instances, to limit the
    // number of file descriptors.
    for seq >= 3 && ndecided(t, pxa, seq - 3) < nMultiPaxos {
      time.Sleep(20 * time.Millisecond)
    }
    for i := 0; i < nMultiPaxos; i++ {
      go execute(pxa,i,seq,(seq * 10) + i)
    }
  }

  for {
    done := true
    for seq := 1; seq < ninst; seq++ {
      if ndecided(t, pxa, seq) < nMultiPaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }
  
  fmt.Printf("  ... Passed\n")
}

func pp(tag string, src int, dst int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  s += "px-" + tag + "-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += strconv.Itoa(src) + "-"
  s += strconv.Itoa(dst)
  return s
}

func cleanpp(tag string, n int) {
  for i := 0; i < n; i++ {
    for j := 0; j < n; j++ {
      ij := pp(tag, i, j)
      os.Remove(ij)
    }
  }
}

func part(t *testing.T, tag string, nMultiPaxos int, p1 []int, p2 []int, p3 []int) {
  cleanpp(tag, nMultiPaxos)
  /*fmt.Printf("new partition\n")
  fmt.Printf("%v \n",p1)
  fmt.Printf("%v \n",p2)
  fmt.Printf("new partition end\n")*/
  pa := [][]int{p1, p2, p3}
  for pi := 0; pi < len(pa); pi++ {
    p := pa[pi]
    for i := 0; i < len(p); i++ {
      for j := 0; j < len(p); j++ {
        ij := pp(tag, p[i], p[j])
        pj := port(tag, p[j])
        err := os.Link(pj, ij)
        if err != nil {
          // one reason this link can fail is if the
          // corresponding MultiPaxos peer has prematurely quit and
          // deleted its socket file (e.g., called px.Kill()).
          t.Fatalf("os.Link(%v, %v): %v\n", pj, ij, err)
        }
      }
    }
  }
}

func TestPartition(t *testing.T) {
  runtime.GOMAXPROCS(4)

  tag := "partition"
  const nMultiPaxos = 5
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  defer cleanup(pxa)
  defer cleanpp(tag, nMultiPaxos)

  for i := 0; i < nMultiPaxos; i++ {
    var pxh []string = make([]string, nMultiPaxos)
    for j := 0; j < nMultiPaxos; j++ {
      if j == i {
        pxh[j] = port(tag, i)
      } else {
        pxh[j] = pp(tag, i, j)
      }
    }
    pxa[i] = Make(pxh, i, nil)
  }
  defer part(t, tag, nMultiPaxos, []int{}, []int{}, []int{})

  seq := 0
  fmt.Printf("Test: No decision if partitioned (no leader)...\n")
  go execute(pxa,1,seq,100)

  part(t, tag, nMultiPaxos, []int{0,2}, []int{1,3}, []int{4})
  checkmax(t, pxa, -1, 0)
  checkmax(t, pxa, seq, 0)
  //give old leader time to detect self as dead if happened to agree earlier
  time.Sleep(PINGWAIT)

  for i := 0; i < nMultiPaxos; i++ {
    if pxa[i].isLeaderAndValid() {
      t.Fatalf("Leader elected without majority!")
    }
  }

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Decision in majority partition with leader...\n")
  part(t, tag, nMultiPaxos, []int{0}, []int{1,2,3}, []int{4})

  //give old leader time to detect self as dead if happened to agree earlier
  time.Sleep(PINGWAIT)

  waitmajority(t, pxa, seq)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: All agree after full heal ...\n")

  go execute(pxa,0,seq,1000) // poke them, doesn't have to be leader
  go execute(pxa,4,seq,1004)

  part(t, tag, nMultiPaxos, []int{0,1,2,3,4}, []int{}, []int{})

  waitn(t, pxa, seq, nMultiPaxos)

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Leader switches partitions (should not bounce on old leader), new leader elected, repeat ...\n")
  
  l := getandcheckleader(t,pxa)
  niters := 5
  for iters := 0; iters < niters; iters++ {
    seq++
    majority := make([]int,3)
    minority := make([]int,2)
    majoritypxa := make([]*MultiPaxos,3)

    //always move the leader to minority to force election
    //group 2
    minority[0] = l
    //group setup
    majptr:= 0
    minptr:= 1
    for i:=0; i<nMultiPaxos; i++{
      if i!=l{
        if majptr < 3{
          majoritypxa[majptr] = pxa[i]
          majority[majptr] = i
          majptr++
        }else{
          minority[minptr] = i
          minptr++
        }
      }
    }
    if len(majority) != 3 || len(minority)!=2{
      t.Fatalf("something went wrong in partition construction")
    }
    //fmt.Printf("new partition \n")
    part(t, tag, nMultiPaxos, majority, minority, []int{})

    time.Sleep(PINGWAIT)

    //wait for leader
    leaderIndexInGroup1 := getandcheckleader(t,majoritypxa)
    l = majority[leaderIndexInGroup1]

    //avoid instance used for leader agreement
    seq++

    //old leader
    go execute(pxa,minority[0],seq,seq*10)

    //new leader
    go execute(pxa,l,seq,(seq * 10) + 1)

    waitmajority(t, pxa, seq)
    if ndecided(t, pxa, seq) > 3 {
      t.Fatalf("too many decided")
    }
    checkval(t,pxa,seq,(seq * 10) + 1)
    
    //fmt.Printf("before heal \n")

    //if other paxos replicas are not in the same partition as the leader they will never get the results
    //so need to join partition and see if they agree
    join:=make([]int,5,5)
    for i:=0; i<nMultiPaxos; i++{
      join[i] = i
    }
    //old issue here was that replicas in minority partition still believe the old leader is the leader
    //because they got no notification RPCs and don't ping other servers to find out differently
    part(t, tag, nMultiPaxos,  join, []int{}, []int{})
    
    newl := getandcheckleader(t,pxa)

    if pxa[newl].me != pxa[l].me{
      t.Fatalf("Leader changed when it wasn't supposed to!")
    }

    //wait for replicas to catch up from leader
    time.Sleep(PINGWAIT)
    
    //should catch up
    waitn(t, pxa, seq, nMultiPaxos)
    checkval(t,pxa,seq,(seq * 10) + 1)
    l = newl

    fmt.Printf("Phase %v out of %v passed... \n",iters+1,niters)
  }

  fmt.Printf("  ... Passed\n")
  
}
func TestPartitionUnreliable(t *testing.T){
  runtime.GOMAXPROCS(4)
  tag := "partition_unreliable"
  const nMultiPaxos = 5
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  defer cleanup(pxa)
  defer cleanpp(tag, nMultiPaxos)

  for i := 0; i < nMultiPaxos; i++ {
    var pxh []string = make([]string, nMultiPaxos)
    for j := 0; j < nMultiPaxos; j++ {
      if j == i {
        pxh[j] = port(tag, i)
      } else {
        pxh[j] = pp(tag, i, j)
      }
    }
    pxa[i] = Make(pxh, i, nil)
  }
  defer part(t, tag, nMultiPaxos, []int{}, []int{}, []int{})

  part(t, tag, nMultiPaxos, []int{0,1,2,3,4}, []int{}, []int{})

  waitn(t,pxa,-1,nMultiPaxos)

  fmt.Printf("Test: One peer switches partitions, unreliable ...\n")
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i].SetUnreliable(true)
  }

  seq := 0
  for iters := 0; iters < 20; iters++ {
    seq++

    part(t, tag, nMultiPaxos, []int{0,1,2}, []int{3,4}, []int{})
    
    //ping wait essentially for replicas to notice leader is dead and vote in a new leader
    getandcheckleader(t,pxa)

    for i := 0; i < nMultiPaxos; i++ {
      go func(seq int, ind int){
        for ndecided(t, pxa, seq) < ((len(pxa) / 2) + 1) && !pxa[ind].dead{
          pxa[ind].Start(seq, (seq * 10) + ind)
          time.Sleep(time.Duration(rand.Int63() % 100) * time.Millisecond)
        }
        fmt.Printf("Detected agreement on %v \n",seq)
      }(seq,i)
    }

    waitn(t, pxa, seq, 3)
    fmt.Printf("Detected majority agreement on %v \n",seq)
    if ndecided(t, pxa, seq) > 3 {
      t.Fatalf("too many decided on %v",seq)
    }
    
    //need for everyone to be in same partiton to receive all data from 
    //leader
    part(t, tag, nMultiPaxos, []int{0,1,2,3,4}, []int{}, []int{})

    getandcheckleader(t,pxa)

    for i := 0; i < nMultiPaxos; i++ {
      pxa[i].SetUnreliable(false)
    }

    fmt.Printf("Detected all agreement on %v \n",seq)
    waitn(t, pxa, seq, 5)
  }

  fmt.Printf("  ... Passed\n")
}
func TestLots(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many requests, changing partitions ...\n")

  tag := "lots"
  const nMultiPaxos = 5
  var pxa []*MultiPaxos = make([]*MultiPaxos, nMultiPaxos)
  defer cleanup(pxa)
  defer cleanpp(tag, nMultiPaxos)

  for i := 0; i < nMultiPaxos; i++ {
    var pxh []string = make([]string, nMultiPaxos)
    for j := 0; j < nMultiPaxos; j++ {
      if j == i {
        pxh[j] = port(tag, i)
      } else {
        pxh[j] = pp(tag, i, j)
      }
    }
    pxa[i] = Make(pxh, i, nil)
    pxa[i].SetUnreliable(true)
  }
  defer part(t, tag, nMultiPaxos, []int{}, []int{}, []int{})

  done := false

  // re-partition periodically
  ch1 := make(chan bool)
  go func() {
    defer func(){ ch1 <- true }()
    for done == false {
      var a [nMultiPaxos]int
      for i := 0; i < nMultiPaxos; i++ {
        a[i] = (rand.Int() % 3)
      }
      pa := make([][]int, 3)
      for i := 0; i < 3; i++ {
        pa[i] = make([]int, 0)
        for j := 0; j < nMultiPaxos; j++ {
          if a[j] == i {
            pa[i] = append(pa[i], j)
          }
        }
      }
      part(t, tag, nMultiPaxos, pa[0], pa[1], pa[2])

      //repartition less often because leader needs to be elected + stabalize
      //before progress can be made
      time.Sleep(time.Duration(rand.Int63() % 3000) * time.Millisecond)
    }
  }()

  seq := 0

  // periodically start a new instance
  ch2 := make(chan bool)
  go func () {
    defer func() { ch2 <- true } ()
    for done == false {
      // how many instances are in progress?
      nd := 0
      for i := 0; i < seq; i++ {
        if ndecided(t, pxa, i) == nMultiPaxos {
          nd++
        }
      }
      if seq - nd < 10 {
        for i := 0; i < nMultiPaxos; i++ {

          //need to retry for this seq just in case the leader died
          go func(seq int, ind int){
            for ndecided(t, pxa, seq) < ((len(pxa) / 2) + 1) && !pxa[ind].dead{
              pxa[ind].Start(seq, rand.Int() % 10)
              time.Sleep(time.Duration(rand.Int63() % 500) * time.Millisecond)
            }
            fmt.Printf("Detected agreement on %v \n",seq)
          }(seq,i)
        }
        seq++
      }
      time.Sleep(time.Duration(rand.Int63() % 300) * time.Millisecond)
    }
  }()

  // periodically check that decisions are consistent
  ch3 := make(chan bool)
  go func() {
    defer func() { ch3 <- true }()
    for done == false {
      for i := 0; i < seq; i++ {
        ndecided(t, pxa, i)
      }
      time.Sleep(time.Duration(rand.Int63() % 100) * time.Millisecond)
    }
  }()

  time.Sleep(35 * time.Second)
  done = true
  <- ch1
  <- ch2
  <- ch3

  // repair, then check that all instances decided.
  for i := 0; i < nMultiPaxos; i++ {
    pxa[i].SetUnreliable(false)
  }
  part(t, tag, nMultiPaxos, []int{0,1,2,3,4}, []int{}, []int{})
  time.Sleep(5 * time.Second)

  for i := 0; i < seq; i++ {
    getandcheckleader(t,pxa)
    waitn(t, pxa, i,nMultiPaxos)
  }

  fmt.Printf("  ... Passed\n")
}
