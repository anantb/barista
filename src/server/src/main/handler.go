package main

/**
 * Barista Handler
 *
 * @author: Anant Bhardwaj
 * @date: 03/23/2014
 */

import "barista"
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

  args := sqlpaxos.OpenArgs{ClientId: strconv.ParseInt(clientid, 10, 64), User: user, Password: password, 
    Database: database, RequestId: strconv.Atoi(requestid)}
  var reply sqlpaxos.OpenReply

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
  args := sqlpaxos.ExecArgs{ClientId: strconv.ParseInt(clientid, 10, 64), RequestId: strconv.Atoi(request_id), Query: query, 
    QueryParams: query_params}
  var reply sqlpaxos.ExecReply
  err := handler.sqlpaxos.ExecuteSQL(&args, &reply)

  if err != nil {
    fmt.Println("Error :", err)
    return nil, err
  }
  return reply.Result, nil
}

func (handler *Handler) CloseConnection(
    con *barista.Connection) (error) {
  client_id := *(con.ClientId)
  request_id := *(con.SeqId)
  args := sqlpaxos.CloseArgs{ClientId: strconv.ParseInt(clientid, 10, 64), RequestId: strconv.Atoi(request_id)}
  var reply sqlpaxos.CloseReply
  return handler.sqlpaxos.Close(&args, &reply)
}