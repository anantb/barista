package main

import "testing"
//import "runtime"
import "strconv"
import "barista"
//import "os"
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
var SP_PORTS = []string {":9011", ":9012", ":9013", ":9014", ":9015"}

func StartServer(me int) {
  binary_protocol_factory := thrift.NewTBinaryProtocolFactoryDefault()
  transport_factory := thrift.NewTTransportFactory()

  binary_transport, err := thrift.NewTServerSocket(ADDRS[me] + BINARY_PORTS[me])
  
  if err != nil {
    fmt.Println("Error opening socket: ", err)
    return
  }

  handler := handler.NewBaristaHandler(ADDRS, me, PG_PORTS, SP_PORTS)
  processor := barista.NewBaristaProcessor(handler)
  binary_server := thrift.NewTSimpleServer4(processor, binary_transport, transport_factory, binary_protocol_factory)

  fmt.Println("Starting the Barista server (Binary Mode) on ", ADDRS[me] + BINARY_PORTS[me])
  go binary_server.Serve()
}

func TestBasic(t *testing.T) {
  const nservers = 5

  // store the addresses of the servers
  //var kvh []string = make([]string, nservers)
  // initialize the addresses

  //var kva []*TSimpleServer = make([]*TSimpleServer, nservers)
  
  for i := 0; i < nservers; i++ {
  	// start servers on the various machines
    StartServer(i)
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

  // retrieve old item from table
  res, err = ck.ExecuteSQL([]string{ADDRS_WITH_PORTS[2]}, con, 
  	"select value from sqlpaxos_test where key='" + key +
  	"'", nil)
  if err != nil || res == nil {
  	t.Fatalf("Error querying table:", err)
  } 

  Print_result_set(res)

  if res != nil && res.Tuples != nil {
    for _, tuple := range *(res.Tuples) {
      for _, cell := range *(tuple.Cells) {
        if val != string(cell[:]) {
          t.Fatalf("Table should be empty: %s", cell)
        }
      }
    }
  }

  err = ck.CloseConnection(ADDRS_WITH_PORTS, con)
	if err != nil {
  	t.Fatalf("Error closing connection:", err)
  }

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Concurrent clients ...\n")

  for iters := 0; iters < 20; iters++ {
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
  			  "insert into sqlpaxos_test values ('b', '" + 
  		    strconv.Itoa(rand.Int()) +"')", nil)
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






