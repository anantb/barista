package main

import "testing"
import "runtime"
import "strconv"
import "barista"
import "os"
import "time"
import "fmt"
import "math/rand"
import "git.apache.org/thrift.git/lib/go/thrift"
import "handler"

var BINARY_PORTS = []string {":9021", ":9022", ":9023", ":9024", ":9025"}
var ADDRS = []string {"128.52.161.243", "128.52.161.243", "128.52.161.243", "128.52.161.243", "128.52.161.243"}
var ADDRS_WITH_PORTS = []string {"128.52.161.243:9021", "128.52.161.243:9022", "128.52.161.243:9023",
  "128.52.161.243:9024", "128.52.161.243:9025"}
var PG_PORTS = []string {"5434", "5435", "5436", "5437", "5438"}
//var SP_PORTS = []string {":9011", ":9012", ":9013", ":9014", ":9015"}

func StartServer(sp_ports []string, me int, unreliable bool) (*thrift.TSimpleServer, *handler.Handler){
  binary_protocol_factory := thrift.NewTBinaryProtocolFactoryDefault()
  transport_factory := thrift.NewTTransportFactory()

  binary_transport, err := thrift.NewTServerSocket(ADDRS[me] + BINARY_PORTS[me])

  if err != nil {
    fmt.Println("Error opening socket: ", err)
    return nil, nil
  }

  handler := handler.NewBaristaHandler(ADDRS[:len(sp_ports)], me, PG_PORTS[:len(sp_ports)], sp_ports, true, unreliable, false)
  processor := barista.NewBaristaProcessor(handler)
  binary_server := thrift.NewTSimpleServer4(processor, binary_transport, transport_factory, binary_protocol_factory)

  //fmt.Println("Starting the Barista server (Binary Mode) on ", ADDRS[me] + BINARY_PORTS[me])
  go binary_server.Serve()
  return binary_server, handler
}

func cleanup(kva []*thrift.TSimpleServer, kvhandler []*handler.Handler) {
  for i := 0; i < len(kva); i++ {
    if kva[i] != nil {
      kva[i].Stop()
      kvhandler[i].Kill()
    }
  }
}

func TestBasic(t *testing.T) {
  const nservers = 5

  // store the addresses of the servers
  var kvh []string = make([]string, nservers)
  var kva []*thrift.TSimpleServer = make([]*thrift.TSimpleServer, nservers)
  var kvhandler []*handler.Handler = make([]*handler.Handler, nservers)
  defer cleanup(kva, kvhandler)

  for i := 0; i < nservers; i++ {
    kvh[i] = port("basic", i)
  }

  for i := 0; i < nservers; i++ {
        // start servers on the various machines
    kva[i], kvhandler[i] = StartServer(kvh, i, false)
  }

  ck := MakeClerk()
  var cka [nservers]*Clerk
  for i := 0; i < nservers; i++ {
    cka[i] = MakeClerk() // this no longer stores all the servers
  }

  fmt.Printf("Test: Basic open/execute/close ...\n")

  con, err := ck.OpenConnection(ADDRS_WITH_PORTS)
  if err != nil {
        t.Fatalf("Error opening connection:", err)
  } else if con == nil {
        t.Fatalf("Error nil connection returned by open:", err)
  }

  fmt.Printf("Passed open ...\n")
  
  _, err = ck.ExecuteSQL(ADDRS_WITH_PORTS, con,
      "DROP TABLE IF EXISTS sqlpaxos_test", nil)
  if err != nil {
    t.Fatalf("Error dropping table:", err)
    return
  }

  _, err = ck.ExecuteSQL(ADDRS_WITH_PORTS[2:4], con,
      "CREATE TABLE IF NOT EXISTS sqlpaxos_test (key text, value text)", nil)
  if err != nil {
    t.Fatalf("Error creating table:", err)
    return
  }

 // count number of rows in the table
  res, err := ck.ExecuteSQL([]string{ADDRS_WITH_PORTS[4]}, con,
        "select count(*) from sqlpaxos_test", nil)
  if err != nil || res == nil {
        t.Fatalf("Error querying table:", err)
  }

  for _, tuple := range *(res.Tuples) {
    for _, cell := range *(tuple.Cells) {
      if "0" != string(cell[:]) {
        t.Fatalf("Table should be empty: %s", cell)
      }
    }
  }

  // insert item into table
  key := "a"
  val := "100"
  res, err = ck.ExecuteSQL(ADDRS_WITH_PORTS[1:3], con,
        "INSERT INTO sqlpaxos_test VALUES ('"+ key +"', '" +
                val +"')", nil)
  if err != nil || res == nil {
        t.Fatalf("Error querying table:", err)
  }

  res, err = ck.ExecuteSQL(ADDRS_WITH_PORTS[1:3], con,
    "INSERT INTO sqlpaxos_test VALUES ('b', '90')", nil)
  if err != nil || res == nil {
    t.Fatalf("Error querying table:", err)
  }

  // retrieve old item from table
  res, err = ck.ExecuteSQL([]string{ADDRS_WITH_PORTS[2]}, con,
        "select value from sqlpaxos_test where key='" + key +
        "'", nil)
  if err != nil || res == nil {
        t.Fatalf("Error querying table:", err)
  }

  //Print_result_set(res)

  if res != nil && res.Tuples != nil {
    for _, tuple := range *(res.Tuples) {
      for _, cell := range *(tuple.Cells) {
        if val != string(cell[:]) {
          t.Fatalf("Table should be empty: %s", cell)
        }
      }
    }
  }
  fmt.Printf("Passed execute ...\n")
  err = ck.CloseConnection(ADDRS_WITH_PORTS, con)
        if err != nil {
        t.Fatalf("Error closing connection:", err)
  }
  fmt.Printf("Passed close ...\n")

  fmt.Printf("  ... Passed\n")  

  fmt.Printf("Test: Concurrent clients ...\n")

  for iters := 0; iters < 2; iters++ {
    const npara = 15
    var ca [npara]chan bool
    for nth := 0; nth < npara; nth++ {
      ca[nth] = make(chan bool)
      go func(me int) {
        defer func() { ca[me] <- true }()
        ci := (rand.Int() % nservers)
        myck := MakeClerk()
        con, err := myck.OpenConnection([]string{ADDRS_WITH_PORTS[ci]})
                if err != nil {
                  t.Fatalf("Error opening connection:", err)
                } else if con == nil {
                  t.Fatalf("Error nil connection returned by open:", err)
                }
        if (rand.Int() % 1000) < 500 {
          // insert
          res, err := myck.ExecuteSQL([]string{ADDRS_WITH_PORTS[ci]}, con,
                          "update sqlpaxos_test set value = '" + strconv.Itoa(rand.Int()) +
          "' WHERE key = 'b'", nil)
                  if err != nil || res == nil {
                        t.Fatalf("Error querying table:", err)
                  }
                  } else {
          // retrieve old item from table
                  res, err = myck.ExecuteSQL([]string{ADDRS_WITH_PORTS[ci]}, con,
                        "select value from sqlpaxos_test where key='" + key +
                          "'", nil)
                  if err != nil || res == nil {
                        t.Fatalf("Error querying table:", err)
                  }
        }
        err = myck.CloseConnection([]string{ADDRS_WITH_PORTS[ci]}, con)
                if err != nil {
                  t.Fatalf("Error closing connection:", err)
                }
      }(nth)
    }

    for nth := 0; nth < npara; nth++ {
      <- ca[nth]
    }

    var va [nservers]string
    for i := 0; i < nservers; i++ {
      con, err := ck.OpenConnection(ADDRS_WITH_PORTS)
      if err != nil {
        t.Fatalf("Error opening connection:", err)
      } else if con == nil {
        t.Fatalf("Error nil connection returned by open:", err)
      }
          res, err := ck.ExecuteSQL(ADDRS_WITH_PORTS, con,
                "select value from sqlpaxos_test where key='" + key +
                  "'", nil)
          if err != nil || res == nil {
                t.Fatalf("Error querying table:", err)
          }
      err = ck.CloseConnection(ADDRS_WITH_PORTS, con)
        if err != nil {
          t.Fatalf("Error closing connection:", err)
      }

      if res != nil && res.Tuples != nil {
        for _, tuple := range *(res.Tuples) {
          for _, cell := range *(tuple.Cells) {
            va[i] = string(cell[:])
          }
        }
        if va[i] != va[0] {
          t.Fatalf("mismatch")
        }
      }
    }
  }

  fmt.Printf("  ... Passed\n")
  time.Sleep(1 * time.Second)
}

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "kv-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}

func pp(tag string, src int, dst int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  s += "kv-" + tag + "-"
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

func part(t *testing.T, tag string, npaxos int, p1 []int, p2 []int, p3 []int) {
  cleanpp(tag, npaxos)

  pa := [][]int{p1, p2, p3}
  for pi := 0; pi < len(pa); pi++ {
    p := pa[pi]
    for i := 0; i < len(p); i++ {
      for j := 0; j < len(p); j++ {
        ij := pp(tag, p[i], p[j])
        pj := port(tag, p[j])
        err := os.Link(pj, ij)
        if err != nil {
          t.Fatalf("os.Link(%v, %v): %v\n", pj, ij, err)
        }
      }
    }
  }
}

func TestPartition(t *testing.T) {
  runtime.GOMAXPROCS(4)

  tag := "partition"
  const nservers = 5
  var kva []*thrift.TSimpleServer = make([]*thrift.TSimpleServer, nservers)
  var kvhandler []*handler.Handler = make([]*handler.Handler, nservers)
  defer cleanup(kva, kvhandler)
  defer cleanpp(tag, nservers)

  for i := 0; i < nservers; i++ {
    var kvh []string = make([]string, nservers)
    for j := 0; j < nservers; j++ {
      if j == i {
        kvh[j] = port(tag, i)
      } else {
        kvh[j] = pp(tag, i, j)
      }
    }
    kva[i], kvhandler[i] = StartServer(kvh, i, false)
  }
  defer part(t, tag, nservers, []int{}, []int{}, []int{})

  var cka [nservers]*Clerk
  var cons [nservers]*barista.Connection
  for i := 0; i < nservers; i++ {
    cka[i] = MakeClerk()//[]string{port(tag, i)})
    //cons[i] = Open(i, cka)
  }

  fmt.Printf("Test: No partition ...\n")

  part(t, tag, nservers, []int{0,1,2,3,4}, []int{}, []int{})
  for i := 0; i < nservers; i++ {
    cons[i] = Open(i, cka)
  }

  Put(2, cka, cons, "b", "100")
  Put(3, cka, cons, "b", "110")
  Get(1, cka, cons, "b", "110")
  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Progress in majority ...\n")

  part(t, tag, nservers, []int{2,3,4}, []int{0,1}, []int{})
  Put(2, cka, cons, "b", "120")
  Get(4, cka, cons, "b", "120")

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: No progress in minority ...\n")
  done0 := false
  done1 := false
  go func() {
    Put(0, cka, cons, "b", "140")
    done0 = true
  }()

  go func() {
    Get(1, cka, cons, "b", "140")
    done1 = true
  }()

  time.Sleep(time.Second)
  if done0 {
    t.Fatalf("Put in minority completed")
  }
  if done1 {
    t.Fatalf("Get in minority completed")
  }

  Get(4, cka, cons, "b", "120")
  Put(3, cka, cons, "b", "160")
  Get(4, cka, cons, "b", "160")

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Completion after heal ...\n")

  part(t, tag, nservers, []int{0,2,3,4}, []int{1}, []int{})
  for iters := 0; iters < 30; iters++ {
    if done0 {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }
  if done0 == false {
    t.Fatalf("Put did not complete")
  }
  if done1 {
    t.Fatalf("Get in minority completed")
  }

  Get(4, cka, cons, "b", "140")
  Get(0, cka, cons, "b", "140")
  
   part(t, tag, nservers, []int{0,1,2}, []int{3,4}, []int{})
  for iters := 0; iters < 100; iters++ {
    if done1 {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }
  if done1 == false {
    t.Fatalf("Get did not complete")
  }
  Get(1, cka, cons, "b", "140")

  part(t, tag, nservers, []int{0,1,2,3,4}, []int{}, []int{})
  for i, _ := range cons {
    Close(i, cka, cons)
  }

  fmt.Printf("  ... Passed\n")
}

func Open(idx int, cka [5]*Clerk) *barista.Connection {
  con, err := cka[idx].OpenConnection([]string{ADDRS_WITH_PORTS[idx]})
  if err != nil {
    fmt.Println("Error opening connection:", err)
  } else if con == nil {
    fmt.Println("Error nil connection returned by open:", err)
  }
  return con
}

func Open10(idx int, cka [3]*Clerk) *barista.Connection {
  con, err := cka[idx].OpenConnection([]string{ADDRS_WITH_PORTS[idx]})
  if err != nil {
    fmt.Println("Error opening connection:", err)
  } else if con == nil {
    fmt.Println("Error nil connection returned by open:", err)
  }
  return con
}

func Open2(ck *Clerk) *barista.Connection {
  con, err := ck.OpenConnection(ADDRS_WITH_PORTS)
  if err != nil {
    fmt.Println("Error opening connection:", err)
  } else if con == nil {
    fmt.Println("Error nil connection returned by open:", err)
  }
  return con
}

func Close(idx int, cka [5]*Clerk, cons [5]*barista.Connection) {
  err := cka[idx].CloseConnection([]string{ADDRS_WITH_PORTS[idx]}, cons[idx])
  if err != nil {
    fmt.Println("Error closing connection:", err)
  }
}

func Close3(idx int, cka [3]*Clerk, cons [3]*barista.Connection) {
  err := cka[idx].CloseConnection([]string{ADDRS_WITH_PORTS[idx]}, cons[idx])
  if err != nil {
    fmt.Println("Error closing connection:", err)
  }
}

func Close2(ck *Clerk, con *barista.Connection) {
  err := ck.CloseConnection(ADDRS_WITH_PORTS, con)
  if err != nil {
    fmt.Println("Error closing connection:", err)
  }
}

func Get(idx int, cka [5]*Clerk, cons [5]*barista.Connection, key string, value string) {
  res, err := cka[idx].ExecuteSQL([]string{ADDRS_WITH_PORTS[idx]}, cons[idx], 
    "select value from sqlpaxos_test where key='" + key + "'", nil)
  if err != nil || res == nil {
    fmt.Println("Error querying table:", err)
  }

  if res != nil && res.Tuples != nil {
    for _, tuple := range *(res.Tuples) {
      for _, cell := range *(tuple.Cells) {
        if string(cell[:]) != value {
          fmt.Println("incorrect output")
        }
      }
    }
  } 
}

func Get10(idx int, cka [3]*Clerk, cons [3]*barista.Connection, key string, value string) {
  res, err := cka[idx].ExecuteSQL([]string{ADDRS_WITH_PORTS[idx]}, cons[idx], 
    "select value from sqlpaxos_test where key='" + key + "'", nil)
  if err != nil || res == nil {
    fmt.Println("Error querying table:", err)
  }

  if res != nil && res.Tuples != nil {
    for _, tuple := range *(res.Tuples) {
      for _, cell := range *(tuple.Cells) {
        if string(cell[:]) != value {
          fmt.Println("incorrect output")
        }
      }
    }
  } 
}

func Get2(ck *Clerk, con *barista.Connection, key string, value string) {
  res, err := ck.ExecuteSQL(ADDRS_WITH_PORTS, con, 
    "select value from sqlpaxos_test where key='" + key + "'", nil)
  if err != nil || res == nil {
    fmt.Println("Error querying table:", err)
  }

  if res != nil && res.Tuples != nil {
    for _, tuple := range *(res.Tuples) {
      for _, cell := range *(tuple.Cells) {
        if string(cell[:]) != value {
          fmt.Println("incorrect output")
        }
      }
    }
  } 
}

func Get3(i int, cka [3]*Clerk, cons [3]*barista.Connection, key string) string {
  res, err := cka[i].ExecuteSQL([]string{ADDRS_WITH_PORTS[i]}, cons[i], 
    "select value from sqlpaxos_test where key='" + key + "'", nil)
  if err != nil || res == nil {
    fmt.Println("Error querying table:", err)
    return ""
  }

  if res != nil && res.Tuples != nil {
    for _, tuple := range *(res.Tuples) {
      for _, cell := range *(tuple.Cells) {
        return string(cell[:])
      }
    }
  } 
  return ""
}

func Get5(i int, cka [5]*Clerk, cons [5]*barista.Connection, key string) string {
  res, err := cka[i].ExecuteSQL([]string{ADDRS_WITH_PORTS[i]}, cons[i], 
    "select value from sqlpaxos_test where key='" + key + "'", nil)
  if err != nil || res == nil {
    fmt.Println("Error querying table:", err)
    return ""
  }

  if res != nil && res.Tuples != nil {
    for _, tuple := range *(res.Tuples) {
      for _, cell := range *(tuple.Cells) {
        return string(cell[:])
      }
    }
  } 
  return ""
}

func Get4(ck *Clerk, con *barista.Connection, key string) string {
  res, err := ck.ExecuteSQL(ADDRS_WITH_PORTS, con, 
    "select value from sqlpaxos_test where key='" + key + "'", nil)
  if err != nil || res == nil {
    fmt.Println("Error querying table:", err)
    return ""
  }

  if res != nil && res.Tuples != nil {
    for _, tuple := range *(res.Tuples) {
      for _, cell := range *(tuple.Cells) {
        return string(cell[:])
      }
    }
  } 
  return ""
}


func Put(idx int, cka [5]*Clerk, cons [5]*barista.Connection, key string, value string) {
  res, err := cka[idx].ExecuteSQL([]string{ADDRS_WITH_PORTS[idx]}, cons[idx], 
    "update sqlpaxos_test set value='" + value + "' where key='" + key + "'", nil)
  if err != nil || res == nil {
    fmt.Println("Error querying table:", err)
  }
}

func Put10(idx int, cka [3]*Clerk, cons [3]*barista.Connection, key string, value string) {
  res, err := cka[idx].ExecuteSQL([]string{ADDRS_WITH_PORTS[idx]}, cons[idx], 
    "update sqlpaxos_test set value='" + value + "' where key='" + key + "'", nil)
  if err != nil || res == nil {
    fmt.Println("Error querying table:", err)
  }
}
func Insert(idx int, cka [5]*Clerk, cons [5]*barista.Connection, key string, value string) {
  res, err := cka[idx].ExecuteSQL([]string{ADDRS_WITH_PORTS[idx]}, cons[idx], 
    "insert into sqlpaxos_test values('" + key + "','"+ value + "')", nil)
  if err != nil || res == nil {
    fmt.Println("Error querying table:", err)
  }
}

func Insert2(ck *Clerk, con *barista.Connection, key string, value string) {
  res, err := ck.ExecuteSQL(ADDRS_WITH_PORTS, con, 
    "insert into sqlpaxos_test values('" + key + "','"+ value + "')", nil)
  if err != nil || res == nil {
    fmt.Println("Error querying table:", err)
  }
}

func Put2(ck *Clerk, con *barista.Connection, key string, value string) {
  res, err := ck.ExecuteSQL(ADDRS_WITH_PORTS, con, 
    "update sqlpaxos_test set value='" + value + "' where key='" + key + "'", nil)
  if err != nil || res == nil {
    fmt.Println("Error querying table:", err)
  }
}

func TestUnreliable(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const nservers = 3
  var kva []*thrift.TSimpleServer = make([]*thrift.TSimpleServer, nservers)
  var kvhandler []*handler.Handler = make([]*handler.Handler, nservers)
  defer cleanup(kva, kvhandler)
  var kvh []string = make([]string, nservers)

  for i := 0; i < nservers; i++ {
    kvh[i] = port("un", i)
  }

  for i := 0; i < nservers; i++ {
    kva[i], kvhandler[i] = StartServer(kvh, i, true)
  }

  ck := MakeClerk()
  con := Open2(ck)
  var cka [nservers]*Clerk
  var cons [nservers]*barista.Connection
  for i := 0; i < nservers; i++ {
    cka[i] = MakeClerk()//[]string{kvh[i]})
    cons[i] = Open10(i, cka)
  }

  fmt.Printf("Test: Basic put/get, unreliable ...\n")

  Put2(ck, con, "a", "aa")
  Get2(ck, con, "a", "aa")

  Put10(1, cka, cons, "a", "aaa")

  Get10(2, cka, cons, "a", "aaa")
  Get10(1, cka, cons, "a", "aaa")
  Get2(ck, con, "a", "aaa")

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Sequence of puts, unreliable ...\n")

  for iters := 0; iters < 6; iters++ {
  const ncli = 5
    var ca [ncli]chan bool
    for cli := 0; cli < ncli; cli++ {
      ca[cli] = make(chan bool)
      go func(me int) {
        ok := false
        defer func() { ca[me] <- ok }()
        myck := MakeClerk()
        con := Open2(myck)
        key := strconv.Itoa(me)
        Get4(myck, con, key)
                Put2(myck, con, key, "1000")
        time.Sleep(100 * time.Millisecond)
        Get2(myck, con, key, "1000")
        ok = true
        Close2(myck, con)
      }(cli)
    }
    for cli := 0; cli < ncli; cli++ {
      x := <- ca[cli]
      if x == false {
        t.Fatalf("failure")
      }
    }
  }

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Concurrent clients, unreliable ...\n")

  for iters := 0; iters < 20; iters++ {
    const ncli = 15
    var ca [ncli]chan bool
    for cli := 0; cli < ncli; cli++ {
      ca[cli] = make(chan bool)
      go func(me int) {
        defer func() { ca[me] <- true }()
        sa := make([]string, len(kvh))
        copy(sa, kvh)
        for i := range sa {
          j := rand.Intn(i+1)
          sa[i], sa[j] = sa[j], sa[i]
        }
        myck := MakeClerk()
        con := Open2(myck)
        if (rand.Int() % 1000) < 500 {
          Put2(myck, con, "b", strconv.Itoa(rand.Int()))
        } else {
          Get4(myck, con, "b")
        }
        Close2(myck, con)
      }(cli)
    }
    for cli := 0; cli < ncli; cli++ {
      <- ca[cli]
    }

    var va [nservers]string
    for i := 0; i < nservers; i++ {
      va[i] = Get3(i, cka, cons, "b")
      if va[i] != va[0] {
        t.Fatalf("mismatch; 0 got %v, %v got %v", va[0], i, va[i])
      }
    }
  }

  Close2(ck, con)
  for idx, _ := range cons {
    Close3(idx, cka, cons)
  }

  fmt.Printf("  ... Passed\n")

  time.Sleep(1 * time.Second)
}

func TestHole(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Tolerates holes in paxos sequence ...\n")

  tag := "hole"
  const nservers = 5
  var kva []*thrift.TSimpleServer = make([]*thrift.TSimpleServer, nservers)
  var kvhandler []*handler.Handler = make([]*handler.Handler, nservers)
  defer cleanup(kva, kvhandler)
  defer cleanpp(tag, nservers)

  for i := 0; i < nservers; i++ {
    var kvh []string = make([]string, nservers)
    for j := 0; j < nservers; j++ {
      if j == i {
        kvh[j] = port(tag, i)
      } else {
        kvh[j] = pp(tag, i, j)
      }
    }
    kva[i], kvhandler[i] = StartServer(kvh, i, false)
  }
  defer part(t, tag, nservers, []int{}, []int{}, []int{})

  for iters := 0; iters < 1; iters++ {
    part(t, tag, nservers, []int{0,1,2,3,4}, []int{}, []int{})

    ck2 := MakeClerk()//[]string{port(tag, 2)})
    con := Open2(ck2)
    Insert2(ck2, con, "q", "q")

    done := false
    const nclients = 10
    for xcli := 0; xcli < nclients; xcli++ {
      Insert2(ck2, con, strconv.Itoa(xcli), "-1")
    }
    var ca [nclients]chan bool
    for xcli := 0; xcli < nclients; xcli++ {
      ca[xcli] = make(chan bool)
      go func(cli int) {
        ok := false
        defer func() { ca[cli] <- ok }()
        var cka [nservers]*Clerk
        var cons [nservers]*barista.Connection
        for i := 0; i < nservers; i++ {
          cka[i] = MakeClerk()//[]string{port(tag, i)})
        }
        for i := 0; i < nservers; i++ {
          cons[i] = Open(i, cka)//[]string{port(tag, i)})
        }
        key := strconv.Itoa(cli)
        last := ""
        Put(0, cka, cons, key, last)
        for done == false {
          ci := (rand.Int() % 2)
          if (rand.Int() % 1000) < 500 {
            nv := strconv.Itoa(rand.Int())
            Put(ci, cka, cons, key, nv)
            last = nv
          } else {
            v := Get5(ci, cka, cons, key)
            if v != last {
              t.Fatalf("%v: wrong value, key %v, wanted %v, got %v",
                cli, key, last, v)
            }
          }
        }
        for i := 0; i < nservers; i++ {
          Close(i, cka, cons)//[]string{port(tag, i)})
        }
        ok = true
      } (xcli)
    }

    time.Sleep(3 * time.Second)
    part(t, tag, nservers, []int{2,3,4}, []int{0,1}, []int{})

    // can majority partition make progress even though
    // minority servers were interrupted in the middle of
    // paxos agreements?
    Get2(ck2, con, "q", "q")

    Put2(ck2, con, "q", "qq")

    Get2(ck2, con, "q", "qq")
      
    // restore network, wait for all threads to exit.
    part(t, tag, nservers, []int{0,1,2,3,4}, []int{}, []int{})
    done = true
    ok := true
    for i := 0; i < nclients; i++ {
      z := <- ca[i]
      ok = ok && z
    }
    if ok == false {
      t.Fatal("something is wrong")
    }
    Get2(ck2, con, "q", "qq")
    Close2(ck2, con)
  }

  fmt.Printf("  ... Passed\n")
}

func TestManyPartition(t *testing.T) {
  runtime.GOMAXPROCS(4)

  fmt.Printf("Test: Many clients, changing partitions ...\n")

  tag := "many"
  const nservers = 5
  var kva []*thrift.TSimpleServer = make([]*thrift.TSimpleServer, nservers)
  var kvhandler []*handler.Handler = make([]*handler.Handler, nservers)
  defer cleanup(kva, kvhandler)
  defer cleanpp(tag, nservers)
  
  for i := 0; i < nservers; i++ {
    var kvh []string = make([]string, nservers)
    for j := 0; j < nservers; j++ {
      if j == i {
        kvh[j] = port(tag, i)
      } else {
        kvh[j] = pp(tag, i, j)
      }
    }
    kva[i], kvhandler[i] = StartServer(kvh, i, true)
  }
  defer part(t, tag, nservers, []int{}, []int{}, []int{})

  var cka [nservers]*Clerk
  var cons [nservers]*barista.Connection
  for i := 0; i < nservers; i++ {
    cka[i] = MakeClerk()//[]string{port(tag, i)})
    //cons[i] = Open(i, cka)
  }

  part(t, tag, nservers, []int{0,1,2,3,4}, []int{}, []int{})
  for i := 0; i < nservers; i++ {
    cons[i] = Open(i, cka)
  }

  done := false

  // re-partition periodically
  ch1 := make(chan bool)
  go func() {
    defer func() { ch1 <- true } ()
    for done == false {
      var a [nservers]int
      for i := 0; i < nservers; i++ {
        a[i] = (rand.Int() % 3)
      }
      pa := make([][]int, 3)
      for i := 0; i < 3; i++ {
        pa[i] = make([]int, 0)
        for j := 0; j < nservers; j++ {
          if a[j] == i {
            pa[i] = append(pa[i], j)
          }
        }
      }
      part(t, tag, nservers, pa[0], pa[1], pa[2])
      time.Sleep(time.Duration(rand.Int63() % 2000) * time.Millisecond)
    }
  }()

  const nclients = 10
  var ca [nclients]chan bool
  for xcli := 0; xcli < nclients; xcli++ {
    Insert(0, cka, cons, strconv.Itoa(xcli), "-1")
  }

  for xcli := 0; xcli < nclients; xcli++ {
    ca[xcli] = make(chan bool)
    go func(cli int) {
      ok := false
      defer func() { ca[cli] <- ok }()
      myck := MakeClerk()
      key := strconv.Itoa(cli)
      last := ""
      con := Open2(myck)
      Put2(myck, con, key, last)
      for done == false {
        if (rand.Int() % 1000) < 500 {
          nv := strconv.Itoa(rand.Int())
          Put2(myck, con, key, nv)
          last = nv
        } else {
          Get2(myck, con, key, last)
        }
      }
      Close2(myck, con)
      ok = true
    } (xcli)
  }

  time.Sleep(20 * time.Second)
  done = true
  <- ch1
  part(t, tag, nservers, []int{0,1,2,3,4}, []int{}, []int{})

  ok := true
  for i := 0; i < nclients; i++ {
    z := <- ca[i]
    ok = ok && z
  }

  for i := 0; i < nservers; i++ {
    Close(i, cka, cons)
  }

  if ok {
    fmt.Printf("  ... Passed\n")
  }
}

