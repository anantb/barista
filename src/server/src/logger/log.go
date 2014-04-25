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

func (lg *Logger) writeToLog(toWrite []byte) error {
  return ioutil.WriteFile(lg.filename, toWrite, 0644)
}

func (lg *Logger) writeToLog(instance SQLPaxosInstance) error {
  return ioutil.WriteFile(lg.filename, toWrite, 0644)
}

func (lg *Logger) readFromLog() SQLPaxosInstance {
  return ioutil.WriteFile(lg.filename, toWrite, 0644)
}

// may need to write a reader to read in the log for recovery

