package main

/**
 * Sample Go Client for Barista
 *
 * @author: Anant Bhardwaj
 * @date: 03/23/2014
 */

import "fmt"
import "barista"
import "git.apache.org/thrift.git/lib/go/thrift"
import "sync"
import crand "crypto/rand"
import "math/big"
import "strconv"

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

//const ADDR = "localhost:9000"
var addrs = []string {"128.52.161.243", "128.52.160.104", "128.52.161.242", "128.52.160.122", "128.52.161.24"}

func main() {  
  clerk := MakeClerk(addrs)
  con := clerk.OpenConnection()
  clerk.ExecuteSQL(con, "SELECT 6.824 as id, 'Distributed Systems' as name", nil) 
  clerk.CloseConnection(con)
}

//
// execute SQL query
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) ExecuteSQL(con *barista.Connection, query string, query_params [][]byte) {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  ck.curRequest++
  done := false

  con.ClientId = &strconv.FormatInt(ck.me, 10)
  con.SeqId = &strconv.Itoa(ck.curRequest)

  // try each server 
  for !done {
     for _, addr := range ck.servers {
        err := ck.executeSQL(addr, query, query_params, con)
	if err != nil {
	   fmt.Println("Error: ", err)
	} else {
	   done = true
	   break
	}
     }
  }

}

//
// open database connection
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) OpenConnection() *barista.Connection {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  ck.curRequest++
  done := false

  user, password, database := "postgres", "postgres", "postgres"
  con_params := barista.ConnectionParams {
     ClientId: &strconv.FormatInt(ck.me, 10),
     SeqId: &strconv.Itoa(ck.curRequest),
     User: &user,
     Password: &password,
     Database: &database }

  // try each server 
  for !done {
     for _, addr := range ck.servers {
        con, err := ck.openConnection(addr, &con_params)
	if err != nil {
	   fmt.Println("Error: ", err)
	} else {
	   return con
	}
     }
  }

  return nil
}

//
// close database connection
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) CloseConnection(con *barista.Connection) {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  ck.curRequest++
  done := false

  con.ClientId = &strconv.FormatInt(ck.me, 10)
  con.SeqId = &strconv.Itoa(ck.curRequest)

  // try each server 
  for !done {
     for _, addr := range ck.servers {
        err := ck.closeConnection(addr, con)
	if err != nil {
	   fmt.Println("Error: ", err)
	} else {
	   done = true
	   break
	}
     }
  }
}


func (ck *Clerk) executeSQL(addr string, query string, query_params [][]byte, con *barista.Connection) error {
  protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
  transport, err := thrift.NewTSocket(addr)

  if err != nil {
     return err
  }

  transport.Open()
  defer transport.Close()

  client := barista.NewBaristaClientFactory(transport, protocolFactory)

  res, err := client.ExecuteSql(con, query, query_params)

  if err != nil {
     return err
  }

  for _, field_name := range *(res.FieldNames) {
     fmt.Printf("%s\t", field_name)
  }

  fmt.Println()

  for _, tuple := range *(res.Tuples) {
     for _, cell := range *(tuple.Cells) {
  	fmt.Printf("%s\t", cell)
     }
  }

  fmt.Println()

  return nil
}

func (ck *Clerk) openConnection(addr string, con_params *barista.ConnectionParams) (*barista.Connection, error) {
  protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
  transport, err := thrift.NewTSocket(addr)

  if err != nil {
     return nil, err
  }

  transport.Open()
  defer transport.Close()

  client := barista.NewBaristaClientFactory(transport, protocolFactory)

  con, err := client.OpenConnection(con_params)

  if err != nil {
     return nil, err
  }

  return con, nil
}

func (ck *Clerk) closeConnection(addr string, con *barista.Connection) error {
  protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
  transport, err := thrift.NewTSocket(addr)

  if err != nil {
     return err
  }

  transport.Open()
  defer transport.Close()

  client := barista.NewBaristaClientFactory(transport, protocolFactory)

  err = client.CloseConnection(con)

  if err != nil {
     return err
  }

  return nil
}

