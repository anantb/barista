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
import "net"
import "strings"

const PORT = ":9000"
var ADDRS = []string {"128.52.161.243:9000", "128.52.160.104:9000", "128.52.161.242:9000", "128.52.160.122:9000", "128.52.161.24:9000"}

func main() {  
  protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
  transportFactory := thrift.NewTTransportFactory()

  addrs, err := net.InterfaceAddrs()
  addr := ""

  if err != nil || len(addrs) < 2 {
     fmt.Println("Error getting ip: ", err)
     addr = "localhost" + PORT
  } else {
     addr = strings.Split(addrs[1].String(), "/")[0] + PORT
  }

  transport, err := thrift.NewTServerSocket(addr)
 
  if err != nil {
    fmt.Println("Error: ", err)
    return
  }

  me := -1
  for i, server := range ADDRS {
     if addr == server {
        me = i
     }
  }

  if me == -1 {
     fmt.Println("Error: I am not listed in the servers")
     return
  }

  handler := NewBaristaHandler(ADDRS, me)
  processor := barista.NewBaristaProcessor(handler)
  server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

  fmt.Println("Starting the Barista server on ", addr)
  server.Serve() 
}