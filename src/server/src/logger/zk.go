package main

import "fmt"
import "time"
import "launchpad.net/gozk/zookeeper"

type StorageManager struct {
  conn *zookeeper.Conn
}

func MakeStorageManager() *StorageManager {
  sm := &StorageManager{}
  return sm
}

func (sm *StorageManager) Open(servers) error {
  conn, session, err := zookeeper.Dial(servers, 5 * time.Second)
  if err != nil {
    fmt.Printf("Can't connect to zookeeper: %v\n", err)
    return err
  }

  // Wait for connection.
  event := <-session
  if event.State != zookeeper.STATE_CONNECTED {
    fmt.Printf("Can't connect to zookeeper: %v\n", event)
    return err
  }

  sm.conn = conn
  return nil
}

func (sm *StorageManager) Close() error {
  return sm.conn.Close()
}

func (sm *StorageManager) Write(path string, data string) error {
  stats, _ := sm.conn.Exists(path)
  var err error

  if stats == nil {
    _, err = sm.conn.Create(path, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
  } else {
    _, err = sm.conn.Set(path, data, -1)
  }

  if err != nil {
    fmt.Printf("Error creating or writing to path (%v): %v\n", path, err)
  }

  return err
}

func (sm *StorageManager) Read(path string) (string, error) {
  data, _, err := sm.conn.Get(path)

  if err != nil {
    fmt.Printf("Error creating or writing to path (%v): %v\n", path, err)
  }

  return data, err
}


func main() {
  servers := "localhost:2181"
  sm, _ := MakeStorageManager()
  sm.Open(servers)
  defer sm.Close()
  _ = sm.Write("/counter2", "000000")
  data, _ := zk.Read("/counter2")
  fmt.Println(data)
}
