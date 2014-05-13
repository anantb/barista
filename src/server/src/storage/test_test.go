package storage

import "testing"

/**
 * ZooKeeper Storage Manager Test
 *
 * @author: Anant Bhardwaj
 * @date: 05/11/2014
 */

func TestBasic(t *testing.T) {
  servers := "localhost:2181"
  sm := MakeStorageManager()
  sm.Open(servers)
  defer sm.Close()
  sm.Create("/edu", "")
  sm.Create("/edu/mit", "")
  sm.Create("/edu/mit/class", "")

  // test create, write, and read
  
  _ = sm.Create("/edu/mit/class/6.824", "Distributed Systems")
  _ = sm.Write("/edu/mit/class/6.831", "UI Design")

  data, _ := sm.Read("/edu/mit/class/6.824")
  if data != "Distributed Systems" {
    t.Fatalf("got=%v wanted=%v", data, "Distributed Systems")
  }

  data, _ = sm.Read("/edu/mit/class/6.831")
  if data != "UI Design" {
    t.Fatalf("got=%v wanted=%v", data, "UI Design")
  }

  sm.Close()
  
  // close the zookeper to check the persistence

  sm.Open(servers)
  data, _ = sm.Read("/edu/mit/class/6.824")
  if data != "Distributed Systems" {
    t.Fatalf("got=%v wanted=%v", data, "Distributed Systems")
  }

  data, _ = sm.Read("/edu/mit/class/6.831")
  if data != "UI Design" {
    t.Fatalf("got=%v wanted=%v", data, "UI Design")
  }

  sm.Close()
}

