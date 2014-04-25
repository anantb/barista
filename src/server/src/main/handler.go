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
  sqlPaxos *sqlpaxos.SQLPaxos
}

// need to store result and error to do duplicate detection

func NewBaristaHandler() *Handler {
  handler := new(Handler)
  handler.sqlPaxos = new(sqlpaxos.SQLPaxos)
  // TODO: StartServer
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

func (handler *Handler) ExecuteSql(con *barista.Connection,
    query string, query_params [][]byte) (*barista.ResultSet, error) {
  args := sqlpaxos.ExecArgs{Query:query, QueryParams:query_params, 
    ClientId:con.client_id, RequestId:con.request_id, Con: *con}
  var reply ExecReply
  sqlPaxos.ExecuteSql(&args, &reply)
  return reply.Result, reply.Error
}