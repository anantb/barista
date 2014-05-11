package storage

//import "fmt"
//import "time"
//import "launchpad.net/gozk/zookeeper"

/**
 * ZooKeeper Storage Manager
 *
 * @author: Anant Bhardwaj
 * @date: 05/11/2014
 */

type StorageManager struct {
  //conn *zookeeper.Conn
}

func MakeStorageManager() *StorageManager {
  sm := &StorageManager{}
  return sm
}

/*func (sm *StorageManager) Open(servers string) error {
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

func (sm *StorageManager) Create(path string, data string) error {
  stats, _ := sm.conn.Exists(path)
  var err error

  if stats == nil {
    _, err = sm.conn.Create(path, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
  }

  if err != nil {
    fmt.Printf("Error creating a node (%v): %v\n", path, err)
  } else {
    //fmt.Printf("Created node (%v): %v\n", path, data)
  }

  return err
}

func (sm *StorageManager) Delete(path string) error {
  
  err := sm.conn.Delete(path, -1)

  if err != nil {
    fmt.Printf("Error writing to node (%v): %v\n", path, err)
  } else {
    //fmt.Printf("Deleted node (%v)", path)
  }

  return err
}

func (sm *StorageManager) Write(path string, data string) error {
  
  _, err := sm.conn.Set(path, data, -1)

  if err != nil {    
    err_create := sm.Create(path, data)
    if err_create != nil {
      fmt.Printf("Error writing to node (%v): %v\n", path, err)
      return err_create
    }
  }

  //fmt.Printf("Written node (%v): %v\n", path, data)
  return err
}

func (sm *StorageManager) Read(path string) (string, error) {
  data, _, err := sm.conn.Get(path)

  if err != nil {
    fmt.Printf("Error reading from node (%v): %v\n", path, err)
  }

  //fmt.Printf("Read node (%v): %v\n", path, data)

  return data, err
}*/
