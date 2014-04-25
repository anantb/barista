package main

/**
 * Barista Handler
 *
 * @author: Anant Bhardwaj
 * @date: 03/23/2014
 */

import "barista"
import "logger"
import "db"
import "fmt"
import "reflect"
import "json"

type Handler struct {
  manager *db.DBManager
  logger *logger.Logger
  sqlPaxos *sqlpaxos.SQLPaxos
}

// need to store result and error to do duplicate detection

func NewBaristaHandler() *Handler {
  handler := new(Handler)
  handler.manager = new(db.DBManager)
  handler.logger = new(logger.Logger)
  handler.sqlPaxos = new(sqlpaxos.SQLPaxos)
  return handler
}

func check(e error) {
  if e != nil {
    panic(e)
  }
}

func (handler *Handler) GetVersion() (float64, error) {
  return barista.VERSION, nil
}

func (handler *Handler) Connect(
    con_params *barista.ConnectionParams) (*barista.Connection, error) {
  
  user := *(con_params.User)
  password := *(con_params.Password)
  database := *(con_params.Database)

  err := handler.manager.Connect(user, password, database)

  if err != nil {
    fmt.Println("Error :", err)
    return nil, err
  }

  con := new(barista.Connection)
  con.User = &user
  con.Database = &database
  return con, nil
}

func (handler *Handler) ExecuteSqlHelper(con *barista.Connection,
    query string, query_params [][]byte) (*barista.ResultSet, error) {

  rows, columns, err := handler.manager.ExecuteSql(query, query_params)
  if err != nil {
    fmt.Println("Error :", err)
    return nil, err
  }

  tuples := []*barista.Tuple{}
  for _, row := range rows {
    tuple := barista.Tuple{Cells: &row}
    tuples = append(tuples, &tuple)
  }
 
  result_set := new(barista.ResultSet)
  result_set.Con = con
  result_set.Tuples = &tuples
  result_set.FieldNames = &columns

  return result_set, nil
}

func (handler *Handler) ExecuteSql(con *barista.Connection,
    query string, query_params [][]byte) (*barista.ResultSet, error) {
  args := sqlpaxos.ExecArgs{Query:query, Query_params:query_params, Client_id:con.client_id, Request_id:con.request_id}
  var reply ExecReply
  sqlPaxos.ExecuteSql(&args, &reply)

  // achieve consensus
  instance := SQLPaxosInstance{Query:query, Query_params:query_params, Client_id:client_id, Request_id:request_id, NoOp:false}
  // pick a sequence number
  seqnum, err := handler.sqlpaxos.AchieveConsensus(instance, seqnum) // get the sequence number assigned to this instance and error if any
  instance.SeqNum = seqnum

  for i=handler.sqlpaxos.done;i<=seqnum;i++ {
    if i == seqnum {
      tmp := instance
    } else {
      tmp := SQLPaxosInstance{NoOp:true}
      instance, err := handler.sqlpaxos.FillHole(tmp, seqnum)
    }
    
    // go through all log instances from done to seqnum. for each:
    query = "BEGIN TRANSACTION;" + query + "; UPDATE SQLPaxosLog SET lastSeqNum=" + i + 
      "; END TRANSACTION;"
    
    // 1. write paxos log to file
    b, err := json.Marshal(instance)
    check(err)
    check(logger.writeToLog(b))
    
    // 2. update transactions and execute on the database
    handler.ExecuteSqlHelper(connection, instance.Query, instance.Query_params)
  }
}