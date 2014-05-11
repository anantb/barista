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
  sm.Create("/test", "")
  sm.Create("/test/_var_tmp_824-1000_px-12568-basic-1", "")
  sm.Create("/test/_var_tmp_824-1000_px-12568-basic-1/store", "")
  sm.Create("/test/_var_tmp_824-1000_px-12568-basic-1/done", "")
  sm.Create("/test/_var_tmp_824-1000_px-12568-basic-1/store/0", "6.824")

  _ = sm.Write("/test/_var_tmp_824-1000_px-12568-basic-1/store/0", "6.824")
  data, _ := sm.Read("/test/_var_tmp_824-1000_px-12568-basic-1/store/0")
  if data != "6.824" {
    t.Fatalf("got=%v wanted=%v", data, "6.824")
  }

  sm.Close(servers)
  sm.Open(servers)
  data, _ := sm.Read("/test/_var_tmp_824-1000_px-12568-basic-1/store/0")
  if data != "6.824" {
    t.Fatalf("got=%v wanted=%v", data, "6.824")
  }
}

