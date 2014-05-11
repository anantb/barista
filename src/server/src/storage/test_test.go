package storage

import "testing"

/**
 * ZooKeeper Storage Manager Test
 *
 * @author: Anant Bhardwaj
 * @date: 05/11/2014
 */

func TestBasicLeaderModified(t *testing.T) {
  servers := "localhost:2181"
  sm := MakeStorageManager()
  sm.Open(servers)
  defer sm.Close()
  sm.Create("/test/")
  sm.Create("/test/localhost")
  _ = sm.Write("/test/localhost", "6.824")
  data, _ := sm.Read("/test/localhost")
  if data != "6.824" {
    t.Fatalf("got=%v wanted=%v", data, "6.824")
  }
}

