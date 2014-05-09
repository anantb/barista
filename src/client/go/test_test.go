package main

import "testing"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"
import "math/rand"

func cleanup(kva []*TSimpleServer) {
  for i := 0; i < len(kva); i++ {
    if kva[i] != nil {
      //kva[i].kill()
      // kill the SQLPaxos server
    }
  }
}

func StartServer(servers []string, me int) {
  protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
  transportFactory := thrift.NewTTransportFactory()
  transport, err := thrift.NewTServerSocket(servers[me])
 
  if err != nil {
    fmt.Println("Error: ", err)
    return
  }

  handler := NewBaristaHandler()
  processor := barista.NewBaristaProcessor(handler)
  server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

  fmt.Println("Starting the Barista server on ", servers[me])
  server.Serve() 
}

func TestBasic(t *testing.T) {
  const nservers = 5

  // store the addresses of the servers
  var kvh []string = make([]string, nservers)
  // initialize the addresses

  var kva []*TSimpleServer = make([]*TSimpleServer, nservers)
  
  defer cleanup(kva)

  for i := 0; i < nservers; i++ {
  	// start servers on the various machines
    kva[i] = StartServer(kvh, i)
  }

  ck := MakeClerk(kvh)
  var cka [nservers]*Clerk
  for i := 0; i < nservers; i++ {
    cka[i] = MakeClerk([]string{kvh[i]})
  }

  fmt.Printf("Test: Basic open/execute/close ...\n")

  con, err := ck.OpenConnection()
  if err != nil {
  	t.Fatalf("Error opening connection:", err)
  } else if con == nil {
  	t.Fatalf("Error nil connection returned by open:", err)
  }

  // create a table
  rs, err := ck.ExecuteSQL(con, 
  	"create table sqlpaxos_test (key varchar(40), value varchar(40))", nil)
  if err != nil {
  	t.Fatalf("Error creatint table:", err)
  }

  // count number of rows in the table
  res, err := ck.ExecuteSQL(con, 
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
  res, err := ck.ExecuteSQL(con, 
  	"insert into sqlpaxos_test values (\'"+ key +"\', \'" + 
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

  for _, tuple := range *(res.Tuples) {
    for _, cell := range *(tuple.Cells) {
      if val != cell {
      	t.Fatalf("Table should be empty: %s", cell)
      }
    }
  }

  err := ck.CloseConnection()
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






