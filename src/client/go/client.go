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

const ADDR = "localhost:9000"

func main() {  
  protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
  transport, err := thrift.NewTSocket(ADDR)

  if err != nil {
    fmt.Println("Error: ", err)
    return
  }

  transport.Open()
  defer transport.Close()

  client := barista.NewBaristaClientFactory(transport, protocolFactory)

  user, password, database := "postgres", "postgres", "postgres"
  con_params := barista.ConnectionParams {
      User: &user,
      Password: &password,
      Database: &database}

  con, err := client.OpenConnection(&con_params)

  if err != nil {
    fmt.Println("Error: ", err)
    return
  }

  res, err := client.ExecuteSql(
      con, "SELECT 6.824 as id, 'Distributed Systems' as name", nil)

  if err != nil {
    fmt.Println("Error: ", err)
    return
  }

  for _, field_name := range *(res.FieldNames) {
    fmt.Printf("%s\t", field_name)
  }

  fmt.Println()

  for _, tuple := range *(res.Tuples) {
    for _, cell := range *(tuple.Cells) {
      fmt.Printf("%s\t", cell)
    }
    fmt.Println()
  }
  
}