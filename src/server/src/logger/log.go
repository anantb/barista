package logger

/**
 * Barista Paxos Logger
 *
 * @author: Manasi Vartak
 * @date: 04/25/2014
 */

import "barista"
import "io/ioutil"

type Logger struct {
  filename string
}

func Make(filename string) *Logger {
  px := &Logger{}
  px.filename = filename
}

func (lg *Logger) writeToLog(op Op) error {
  b, err := json.Marshal(op)
  check(err) 
  return ioutil.WriteFile(lg.filename, b, 0644)
}

func (lg *Logger) readFromLog() []byte, error {
  return ioutil.ReadFile(lg.filename, toWrite, 0644)
}

