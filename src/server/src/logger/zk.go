package main

import (
  "fmt"
  "launchpad.net/gozk/zookeeper"
)

func main() {
  zk, session, err := zookeeper.Dial("localhost:2181", 5e9)
  if err != nil {
    fmt.Printf("Can't connect: %v\n", err)
  }
  defer zk.Close()

  // Wait for connection.
  event := <-session
  if event.State != zookeeper.STATE_CONNECTED {
    fmt.Printf("Can't connect: %v\n", event)
  }

  _, err = zk.Create("/counter", "0", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
  if err != nil {
    fmt.Printf("Can't create counter: %v\n", err)
  } else {
    fmt.Println("Counter created!")
  }
}
