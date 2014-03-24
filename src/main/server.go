package main

/**
 * Barista Server
 *
 * @author: Anant Bhardwaj
 * @date: 03/23/2014
 *
 */

import (
  "fmt"
  "barista"
  "git.apache.org/thrift.git/lib/go/thrift"
)

func main() {
  const addr = "localhost:9000"
  
  protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
  transportFactory := thrift.NewTTransportFactory()
  transport, err := thrift.NewTServerSocket(addr)
 
  if err != nil {
    fmt.Println("Error: ", err)
    return
  }

  handler := NewBaristaHandler()
  processor := barista.NewBaristaProcessor(handler)
  server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

  fmt.Println("Starting the Barista server on ", addr)
  server.Serve()
  
}