package logger

/**
 * Barista Paxos Logger
 *
 * @author: Manasi Vartak
 * @date: 04/25/2014
 */

import "io/ioutil"
import "os"
//import "json"

CONST log_path = "/tmp/sqlpaxos/"

type Logger struct {
  filename string
}

func Make(filename string, port string) *Logger {
  logger := &Logger{}
  logger.filename = filename
  err := os.MkdirAll(log_path + port, 0777)
  if err != nil {
    fmt.Println("Error creating log dir: " + "/tmp/sqlpaxos/" + port)
    fmt.Println(err)
  }
  filename = log_path + port + "/" + filename
  return logger
}

func (lg *Logger) WriteToLog(text string) error {
  f, err := os.OpenFile(lg.filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
  if err != nil {
      return err
  }
  defer f.Close()
  _, err = f.WriteString(text + "\n") 
  return err
}

func (lg *Logger) ReadFromLog() ([]byte, error) {
  return ioutil.ReadFile(lg.filename)
}

