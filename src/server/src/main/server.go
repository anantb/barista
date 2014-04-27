package main

/**
 * Barista Server
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
  transportFactory := thrift.NewTTransportFactory()
  transport, err := thrift.NewTServerSocket(ADDR)
 
  if err != nil {
    fmt.Println("Error: ", err)
    return
  }

  handler := NewBaristaHandler()
  processor := barista.NewBaristaProcessor(handler)
  server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

  fmt.Println("Starting the Barista server on ", ADDR)
  server.Serve() 
}