package main

import "testing"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"
import "math/rand"

const BINARY_PORTS = {":9021", ":9022", ":9023", ":9024", ":9025"}
var ADDRS = []string {"128.52.161.243", "128.52.161.243", "128.52.161.243", "128.52.161.243", "128.52.161.243"}
var ADDRS_WITH_PORTS = []string {"128.52.161.243:9021", "128.52.161.243:9022", "128.52.161.243:9023", 
  "128.52.161.243:9024", "128.52.161.243:9025"}
var PG_PORTS = []string {"5434", "5435", "5436", "5437", "5438"}
var SP_PORTS = []string {":9011", ":9012", ":9013", ":9014", ":9015"}

func StartServer(servers []string, me int) {
  pbinary_protocol_factory := thrift.NewTBinaryProtocolFactoryDefault()
  json_protocol_factory := thrift.NewTJSONProtocolFactory()
  transport_factory := thrift.NewTTransportFactory()

  binary_transport, err := thrift.NewTServerSocket(ADDRS[me] + PORT_BINARY[me])
  
  if err != nil {
    fmt.Println("Error opening socket: ", err)
    return
  }

  handler := NewBaristaHandler(ADDRS, me, PG_PORTS, SP_PORTS)
  processor := barista.NewBaristaProcessor(handler)
  binary_server := thrift.NewTSimpleServer4(processor, binary_transport, transport_factory, binary_protocol_factory)
  json_server := thrift.NewTSimpleServer4(processor, json_transport, transport_factory, json_protocol_factory)

  fmt.Println("Starting the Barista server (Binary Mode) on ", ADDRS[me] + BINARY_PORTS[me])
  go binary_server.Serve()
}

func TestBasic(t *testing.T) {
  const nservers = 5

  // store the addresses of the servers
  var kvh []string = make([]string, nservers)
  // initialize the addresses

  var kva []*TSimpleServer = make([]*TSimpleServer, nservers)
  
  for i := 0; i < nservers; i++ {
  	// start servers on the various machines
    kva[i] = StartServer(kvh, i)
  }

  ck := MakeClerk(kvh)
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

  _, err = clerk.ExecuteSQL(ADDRS_WITH_PORTS con,
      "CREATE TABLE IF NOT EXISTS sqlpaxos_test (key text, value text)", nil)
  if err != nil {
    t.Fatalf("Error creating table:", err)
    return
  }

  // count number of rows in the table
  res, err := ck.ExecuteSQL(ADDRS_WITH_PORTS, con, 
  	"select count(*) from sqlpaxos_test", nil)
  if err != nil || res == nil {
  	t.Fatalf("Error querying table:", err)
  } 

  for _, tuple := range *(res.Tuples) {
    for _, cell := range *(tuple.Cells) {
      if "0" != cell {
      	t.Fatalf("Table should be empty: %s", cell)
      }
    }
  }

  // insert item into table
  key := "a"
  val := "100"
  res, err := ck.ExecuteSQL(ADDRS_WITH_PORTS,con, 
  	"INSERT INTO sqlpaxos_test VALUES (\'"+ key +"\', \'" + 
  		value +"\')", nil)
  if err != nil || res == nil {
  	t.Fatalf("Error querying table:", err)
  } 

  // retrieve old item from table
  res, err := ck.ExecuteSQL(con, 
  	"select value from sqlpaxos_test where key=\'" + key 
  	  + "\'", nil)
  if err != nil || res == nil {
  	t.Fatalf("Error querying table:", err)
  } 

  ck.Print_result_set(res)

  if res != nil && res.Tuples != nil {
    for _, tuple := range *(res.Tuples) {
      for _, cell := range *(tuple.Cells) {
        if val != cell {
          t.Fatalf("Table should be empty: %s", cell)
        }
      }
    }
  }

  err := ck.CloseConnection(ADDRS_WITH_PORTS)
	if err != nil {
  	t.Fatalf("Error closing connection:", err)
  }

  fmt.Printf("  ... Passed\n")
  return

  fmt.Printf("Test: Concurrent clients ...\n")

  for iters := 0; iters < 20; iters++ {
    const npara = 15
    var ca [npara]chan bool
    for nth := 0; nth < npara; nth++ {
      ca[nth] = make(chan bool)
      go func(me int) {
        defer func() { ca[me] <- true }()
        ci := (rand.Int() % nservers)
        myck := MakeClerk([]string{kvh[ci]})
        con, err := ck.OpenConnection()
		if err != nil {
		  t.Fatalf("Error opening connection:", err)
		} else if con == nil {
		  t.Fatalf("Error nil connection returned by open:", err)
		}
        if (rand.Int() % 1000) < 500 {
          // insert
          res, err := ck.ExecuteSQL(con, 
  			"insert into sqlpaxos_test values (\'"+ key +"\', \'" + 
  		    strconv.Itoa(rand.Int())) +"\')", nil)
		  if err != nil || res == nil {
		  	t.Fatalf("Error querying table:", err)
		  } 
  		} else {
          // retrieve old item from table
		  res, err := ck.ExecuteSQL(con, 
		  	"select value from sqlpaxos_test where key=\'" + key 
		  	  + "\'", nil)
		  if err != nil || res == nil {
		  	t.Fatalf("Error querying table:", err)
		  } 
        }
        err := ck.CloseConnection()
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
	  res, err := ck.ExecuteSQL(con, 
	  	"select value from sqlpaxos_test where key=\'" + key 
	  	  + "\'", nil)
	  if err != nil || res == nil {
	  	t.Fatalf("Error querying table:", err)
	  } 
	  for _, tuple := range *(res.Tuples) {
        for _, cell := range *(tuple.Cells) {
          va[i] = cell
    	}
      }
      
      if va[i] != va[0] {
        t.Fatalf("mismatch")
      }
    }
  }

  fmt.Printf("  ... Passed\n")

  time.Sleep(1 * time.Second)
}






