package logger

/**
 * Barista Paxos Logger
 *
 * @author: Manasi Vartak
 * @date: 04/25/2014
 */

import "io/ioutil"
//import "json"

type Logger struct {
  filename string
}

func Make(filename string) *Logger {
  logger := &Logger{}
  logger.filename = filename
  return logger
}

func (lg *Logger) WriteToLog(b []byte) error {
  return ioutil.WriteFile(lg.filename, b, 0644)
}

func (lg *Logger) ReadFromLog() ([]byte, error) {
  return ioutil.ReadFile(lg.filename)
}

