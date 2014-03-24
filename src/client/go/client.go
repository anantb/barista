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

const addr = "localhost:9000"

func main() {  
  protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
  transport, err := thrift.NewTSocket(addr)

  if err != nil {
    fmt.Println("Error: ", err)
    return
  }

  transport.Open()
  defer transport.Close()

  client := barista.NewBaristaClientFactory(transport, protocolFactory)

  user, password, database := "postgres", "postgres", "postgres"
  con_params := barista.ConnectionParams{
      User: &user,
      Password: &password,
      Database: &database}

  con, _ := client.Connect(&con_params)
  res, _ := client.ExecuteSql(con, "SELECT 6824", nil)

  fmt.Println(res)
}