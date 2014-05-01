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

type Logger struct {
  filename string
}

func Make(filename string) *Logger {
  logger := &Logger{}
  logger.filename = filename
  return logger
}

func (lg *Logger) WriteToLog(text string) error {
  f, err := os.OpenFile(lg.filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
  if err != nil {
      return err
  }
  defer f.Close()
  _, err = f.WriteString(text) 
  return err
}

func (lg *Logger) ReadFromLog() ([]byte, error) {
  return ioutil.ReadFile(lg.filename)
}

