package main

import "fmt"
import "launchpad.net/gozk/zookeeper"

type ZK struct {
  Conn *zookeeper.Conn
}

func Make() (*ZK, error) {
  zk := &ZK{}
  conn, session, err := zookeeper.Dial("localhost:2181", 5e9)
  if err != nil {
    fmt.Printf("Can't connect to zookeeper: %v\n", err)
    return nil, err
  }
  defer conn.Close()

  // Wait for connection.
  event := <-session
  if event.State != zookeeper.STATE_CONNECTED {
    fmt.Printf("Can't connect to zookeeper: %v\n", event)
    return nil, err
  }
  zk.Conn = conn
  return zk, nil
}

func (zk *ZK) Write(path string, data string) error {
  stats, _ := zk.Conn.Exists(path)
  var err error
  if stats == nil {
    _, err = zk.Conn.Create(path, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
  } else {
    _, err = zk.Conn.Set(path, data, -1)
  }

  if err != nil {
    fmt.Printf("Error creating or writing to path (%v): %v\n", path, err)
  }

  return err
}

func (zk *ZK) Read(path string) (string, error) {
  data, _, err := zk.Conn.Get(path)
  return data, err
}


func main() {
  zk, _ := Make()
  _ = zk.Write("/counter", "000000")
  data, _ := zk.Read("/counter")
  fmt.Println(data)
}
