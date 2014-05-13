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
import "crypto/rand"
import "math/big"
import "strconv"
import "time"

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
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

// List of machines running on the server forming a paxos group
// 128.52.161.243:9000 -- barista-1
// 128.52.160.104:9000 -- barista-2
// 128.52.161.242:9000 -- barista-3
// 128.52.160.122:9000 -- barista-4
// 128.52.161.24:9000 -- barista-4

// to demonstrate external consistency we create three groups
var peers = []string {"128.52.161.243:9000", "128.52.160.104:9000", "128.52.161.242:9000", "128.52.160.122:9000", "128.52.161.24:9000"}

func main() {  
  clerk := MakeClerk()

  // =========================================
  // demo strong consistency & fault-tolerance
  // =========================================

  // put different records on different machine
  // all operations should appear in same order on all machines
  clerk.erase_and_write_to_different_peers(peers)  

  // =========================================
  // demo recovery
  // =========================================

  // ---------------------------------------------
  // SETUP : kill barista-1
  // ---------------------------------------------

  s := "no"
  fmt.Println("Kill barista-1 and type 'yes' to continue")
  for s != "yes" {
    fmt.Scan(&s)
  }

  // delete a record from barista-2
  clerk.execute_on_one_peer(peers, 1, "DELETE FROM courses WHERE id='6.831'")
  fmt.Println("=========================")

  // insert a record on barista-3
  clerk.execute_on_one_peer(peers, 2, "INSERT INTO courses values('CS 142', 'Web Applications')")
  fmt.Println("=========================")

  // ---------------------------------------------
  // SETUP: reboot barista-1 (the crashed machine)
  // ---------------------------------------------

  s = "no"
  fmt.Println("Restart barista-1 and type 'yes' to continue")
  for s != "yes" {
    fmt.Scan(&s)
  }

  // execute an operation on the crashed machine (barista-1)
  // all missing operations along with this one should show up
  clerk.execute_on_one_peer(peers, 0, "INSERT INTO courses values('CS 229', 'Machine Learning')")
  

}

func (clerk *Clerk) erase_and_write_to_different_peers(peers []string) {
  var con *barista.Connection
  var err error

  // open connection to barista-1
  con, err = clerk.OpenConnection([]string {peers[0]})
  if err != nil {
    fmt.Println(err)
    return
  }

  // create the table on barista-2  
  _, err = clerk.ExecuteSQL([]string {peers[1]}, con,
      "CREATE TABLE IF NOT EXISTS courses (id text, name text)", nil)
  if err != nil {
    fmt.Println(err)
    return
  }
  
  // erase all the data on barista-3  
  _, err = clerk.ExecuteSQL([]string {peers[2]}, con, "DELETE FROM courses", nil)
  if err != nil {
    fmt.Println(err)
    return
  }

  // insert a record to barista-1 
  _, err = clerk.ExecuteSQL([]string {peers[0]}, con,
      "INSERT INTO courses values('6.831', 'UID')", nil)
  if err != nil {
    fmt.Println(err)
    return
  }

  // insert a different record to barista-2  
  _, err = clerk.ExecuteSQL([]string {peers[1]}, con,
      "INSERT INTO courses values('6.830', 'Databases')", nil)
  if err != nil {
    fmt.Println(err)
    return
  }

  // insert a differnet record to barista-3 
  _, err = clerk.ExecuteSQL([]string {peers[2]}, con,
      "INSERT INTO courses values('6.824', 'Distributed Systems')", nil)
  if err != nil {
    fmt.Println(err)
    return
  }

  // all queries should apply in the same order on all the machines
  // all the three records should print regardless of whichever 
  // machine/group you query 

  // print all the records on all peers
  for i, peer := range peers {
    fmt.Printf("Machine: barista-%v (%v)\n", i+1, peer)
    res, err := clerk.ExecuteSQL([]string {peer}, con, "SELECT * FROM courses", nil)
    if err != nil {
      fmt.Println(err)
      return
    }
    
    print_result_set(res)
  }

  
  
  // close the connection to barista-1
  // it should close this client's connection from all machines  
  err = clerk.CloseConnection([]string {peers[0]}, con)
  if err != nil {
    fmt.Println(err)
    return
  }
}

func (clerk *Clerk) execute_on_one_peer(peers []string, k int, query string) {
  var con *barista.Connection
  var err error

  // open connection to barista-1
  con, err = clerk.OpenConnection([]string {peers[k]})
  if err != nil {
    fmt.Println(err)
    return
  }

  // insert a record to barista-1 
  _, err = clerk.ExecuteSQL([]string {peers[k]}, con,
      query, nil)
  if err != nil {
    fmt.Println(err)
    return
  }

  // print all the records on all peers
  for i, peer := range peers {
    fmt.Printf("Machine: barista-%v (%v)\n", i+1, peer)
    res, err := clerk.ExecuteSQL([]string {peer}, con, "SELECT * FROM courses", nil)
    if err != nil {
      fmt.Println(err)
      return
    }
    
    print_result_set(res)
  }

  // close the connection to barista-1
  // it should close this client's connection from all machines  
  err = clerk.CloseConnection([]string {peers[k]}, con)
}

// open database connection
func (ck *Clerk) OpenConnection(
  addrs []string) (*barista.Connection, error) {

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

  var err error

  for _, addr := range addrs {  
    con, err := open_connection(addr, &con_params)
    if err == nil {
      return con, nil
    }
  }

  return nil, err

}


// execute SQL query
func (ck *Clerk) ExecuteSQL(
    addrs []string, con *barista.Connection, query string,
    query_params [][]byte) (*barista.ResultSet, error) {

  ck.mu.Lock()
  defer ck.mu.Unlock()

  ck.curRequest++

  clientId := strconv.FormatInt(ck.me, 10)
  seqId := strconv.Itoa(ck.curRequest)

  con.ClientId = &clientId
  con.SeqId = &seqId

  var err error
  //fmt.Printf("Querying: %v, %v\n", query, addrs)


  for _, addr := range addrs {
    //fmt.Printf("Querying: %v\n", addr)
    res, err :=  execute_sql(addr, query, query_params, con)
    if err == nil {
      return res, err
    } else {
      fmt.Println(err) 
    }
  }

  return nil, err
}

// close database connection
func (ck *Clerk) CloseConnection(
  addrs []string, con *barista.Connection) error {

  ck.mu.Lock()
  defer ck.mu.Unlock()

  ck.curRequest++

  clientId := strconv.FormatInt(ck.me, 10)
  seqId := strconv.Itoa(ck.curRequest)

  con.ClientId = &clientId
  con.SeqId = &seqId

  var err error

  for _, addr := range addrs {
    err = close_connection(addr, con)
    if err == nil {
      return nil
    }   
  }

  return err
}


func execute_sql(
    addr string, query string, query_params [][]byte,
    con *barista.Connection) (*barista.ResultSet, error) {

  protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
  transport, err := thrift.NewTSocket(addr)
  transport.SetTimeout(time.Duration(15)*time.Second)
  if err != nil {
    fmt.Println(err)
    return nil, err
  }

  transport.Open()
  defer transport.Close()

  client := barista.NewBaristaClientFactory(transport, protocolFactory)

  res, err := client.ExecuteSql(con, query, query_params)

  if err != nil {
     return nil, err
  }

  return res, nil
}

func open_connection(addr string,
    con_params *barista.ConnectionParams) (*barista.Connection, error) {

  protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
  transport, err := thrift.NewTSocket(addr)
  transport.SetTimeout(time.Duration(15)*time.Second)

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

func close_connection(addr string, con *barista.Connection) error {

  protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
  transport, err := thrift.NewTSocket(addr)
  transport.SetTimeout(time.Duration(15)*time.Second)

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

func Print_result_set(res *barista.ResultSet) {
  print_result_set(res)
}

func print_result_set(res *barista.ResultSet) {

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
      fmt.Println()
    }
  }

  fmt.Println()
}