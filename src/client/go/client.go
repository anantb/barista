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
  mu sync.Mutex
  me int64 // passed as clientId
  curRequest int
}


func MakeClerk() *Clerk {
  ck := new(Clerk)
  ck.me = nrand()
  ck.curRequest = 0

  return ck
}

var addrs_1 = []string {"128.52.161.243:9000", "128.52.160.104:9000"}
var addrs_2 = []string {"128.52.161.242:9000", "128.52.160.122:9000"}
var addrs_3 = []string {"128.52.161.24:9000"}


func main() {  
  clerk := MakeClerk()

  var con *barista.Connection
  var err error

  // clerk should keep retrying to servers in a round-robin function
  for _, addr := range addrs_2 {
    con, err = clerk.OpenConnection(addr)
    if err == nil {
      break
    }
  }

  for _, addr := range addrs_2 {
    err := clerk.ExecuteSQL(addr, con, "CREATE TABLE IF NOT EXISTS courses (id text, name text)", nil)
    if err == nil {
      break
    }
  }

  for _, addr := range addrs_3 {
    err := clerk.ExecuteSQL(addr, con, "DELETE FROM courses", nil)
    if err == nil {
      break
    }
  }

  for _, addr := range addrs_1 {
    err := clerk.ExecuteSQL(addr, con, "INSERT INTO courses values('6.824', 'Distributed Systems')", nil)
    if err == nil {
      break
    }
  }

  for _, addr := range addrs_1 {
    err := clerk.ExecuteSQL(addr, con, "SELECT * FROM courses", nil)
    if err == nil {
      break
    }
  }

  for _, addr := range addrs_1 {
    err := clerk.CloseConnection(addr, con)
    if err == nil {
      break
    }
  }
}

// open database connection
func (ck *Clerk) OpenConnection(addr string) (*barista.Connection, error) {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  ck.curRequest++

  clientId := strconv.FormatInt(ck.me, 10)
  seqId := strconv.Itoa(ck.curRequest)

  user, password, database := "postgres", "postgres", "postgres"
  con_params := barista.ConnectionParams {
     ClientId: &clientId,
     SeqId: &seqId,
     User: &user,
     Password: &password,
     Database: &database }

  con, err := ck.openConnection(addr, &con_params)
  if err != nil {
     return nil, err
  }

  return con, nil
}


// execute SQL query
func (ck *Clerk) ExecuteSQL(addr string, con *barista.Connection, query string, query_params [][]byte) error {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  ck.curRequest++

  clientId := strconv.FormatInt(ck.me, 10)
  seqId := strconv.Itoa(ck.curRequest)

  con.ClientId = &clientId
  con.SeqId = &seqId
  
  err := ck.executeSQL(addr, query, query_params, con)
  if err != nil {
     return err
  }

  return nil
}

// close database connection
func (ck *Clerk) CloseConnection(addr string, con *barista.Connection) error {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  ck.curRequest++

  clientId := strconv.FormatInt(ck.me, 10)
  seqId := strconv.Itoa(ck.curRequest)

  con.ClientId = &clientId
  con.SeqId = &seqId

  err := ck.closeConnection(addr, con)
  if err != nil {
     return err
  } 

  return nil
 
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

  if res != nil && res.FieldNames != nil {
     for _, field_name := range *(res.FieldNames) {
        fmt.Printf("%s\t", field_name)
     }
  }

  fmt.Println()

  if res != nil && res.Tuples != nil {
     for _, tuple := range *(res.Tuples) {
        for _, cell := range *(tuple.Cells) {
       fmt.Printf("%s\t", cell)
        }
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

