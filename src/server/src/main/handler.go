package main

/**
 * Barista Handler
 *
 * @author: Anant Bhardwaj
 * @date: 03/23/2014
 */

import "barista"
import "db"
import "fmt"
import "sqlpaxos"

type Handler struct {
  sqlpaxos *sqlpaxos.SQLPaxos
}

func NewBaristaHandler(servers []string, me int) *Handler {
  handler := new(Handler)
  handler.sqlpaxos = sqlpaxos.StartServer(servers, me)
  return handler
}

func (handler *Handler) GetVersion() (float64, error) {
  return barista.VERSION, nil
}

func (handler *Handler) OpenConnection(
    con_params *barista.ConnectionParams) (*barista.Connection, error) {
  clientid := *(con_params.ClientId)
  user := *(con_params.User)
  password := *(con_params.Password)
  database := *(con_params.Database)
  requestid := *(con_params.SeqId)

  args := OpenArgs{ClientId: clientid, Username: user, Password: password, 
    Database: database, RequestId: requestid}
  var reply OpenReply

  err := handler.sqlpaxos.Open(&args, &reply)

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
  client_id := *(con.ClientId)
  request_id := *(con.SeqId)
  args := ExecArgs{ClientId: client_id, RequestId: request_id, Query: query, 
    Query_params: query_params}
  var reply ExecReply
  err := handler.sqlpaxos.ExecuteSql(&args, &reply)

  if err != nil {
    fmt.Println("Error :", err)
    return nil, err
  }
  return reply.Result, nil
}

func (handler *Handler) CloseConnection(
    con *barista.Connection) (error) {
  args := CloseArgs{ClientId: *(con_params.ClientId), RequestId: *(con_params.SeqId)}
  var reply CloseReply
  return handler.sqlpaxos.Close(&args, &reply)
}