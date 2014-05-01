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
  //return ioutil.WriteFile(lg.filename, b, 0644)
  f, err := os.OpenFile(lg.filename, os.O_APPEND, 0644) 
  n, err := f.WriteString(text) 
  f.Close()
}

func (lg *Logger) ReadFromLog() ([]byte, error) {
  return ioutil.ReadFile(lg.filename)
}

