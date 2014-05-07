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
import "strconv"
import "database/sql"

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

  client_id, err := strconv.ParseInt(clientid, 10, 64)
  if err != nil {
    fmt.Println("Error: ", err)
  }

  request_id, err := strconv.Atoi(requestid)
  if err != nil {
    fmt.Println("Error: ", err)
  }

  args := sqlpaxos.OpenArgs{ClientId: client_id, User: user, Password: password, 
    Database: database, RequestId: request_id}
  var reply sqlpaxos.OpenReply

  err = handler.sqlpaxos.Open(&args, &reply)

  if err != nil {
    fmt.Println("Error :", err)
    return nil, err
  }

  con := new(barista.Connection)
  con.User = &user
  con.Database = &database
  con.ClientId = &clientid
  return con, nil
}

func (handler *Handler) ExecuteSql(con *barista.Connection,
    query string, query_params [][]byte) (*barista.ResultSet, error) {
  clientid := *(con.ClientId)
  requestid := *(con.SeqId)

  client_id, err := strconv.ParseInt(clientid, 10, 64)
  if err != nil {
    fmt.Println("Error: ", err)
  }

  request_id, err := strconv.Atoi(requestid)
  if err != nil {
    fmt.Println("Error: ", err)
  }
  args := sqlpaxos.ExecArgs{ClientId: client_id, RequestId: request_id, Query: query, 
    QueryParams: query_params}
  var reply sqlpaxos.ExecReply
  err = handler.sqlpaxos.ExecuteSQL(&args, &reply)

  if err != nil {
    fmt.Println("Error :", err)
    return nil, err
  }
  return reply.Result, nil
}

func (handler *Handler) ExecuteSqlTxn(con *barista.Connection,
    query string, query_params [][]byte, txn *sql.Tx) (*barista.ResultSet, error) {
  clientid := *(con.ClientId)
  requestid := *(con.SeqId)

  client_id, err := strconv.ParseInt(clientid, 10, 64)
  if err != nil {
    fmt.Println("Error: ", err)
  }

  request_id, err := strconv.Atoi(requestid)
  if err != nil {
    fmt.Println("Error: ", err)
  }
  args := sqlpaxos.ExecTxnArgs{ClientId: client_id, RequestId: request_id, Query: query, 
    QueryParams: query_params, Txn: txn}
  var reply sqlpaxos.ExecTxnReply
  err = handler.sqlpaxos.ExecuteSQLTxn(&args, &reply)

  if err != nil {
    fmt.Println("Error :", err)
    return nil, err
  }
  return reply.Result, nil
}

func (handler *Handler) BeginTxn(con *barista.Connection) (*sql.Tx, error) {
  clientid := *(con.ClientId)
  requestid := *(con.SeqId)

  client_id, err := strconv.ParseInt(clientid, 10, 64)
  if err != nil {
    fmt.Println("Error: ", err)
  }

  request_id, err := strconv.Atoi(requestid)
  if err != nil {
    fmt.Println("Error: ", err)
  }
  args := sqlpaxos.BeginTxnArgs{ClientId: client_id, RequestId: request_id}
  var reply sqlpaxos.BeginTxnReply
  err = handler.sqlpaxos.BeginTxn(&args, &reply)

  if err != nil {
    fmt.Println("Error :", err)
    return nil, err
  }
  return reply.Txn, nil
}

func (handler *Handler) CommitTxn(con *barista.Connection, txn *sql.Tx) error {
  clientid := *(con.ClientId)
  requestid := *(con.SeqId)

  client_id, err := strconv.ParseInt(clientid, 10, 64)
  if err != nil {
    fmt.Println("Error: ", err)
  }

  request_id, err := strconv.Atoi(requestid)
  if err != nil {
    fmt.Println("Error: ", err)
  }
  args := sqlpaxos.CommitTxnArgs{ClientId: client_id, RequestId: request_id, Txn: txn}
  var reply sqlpaxos.CommitTxnReply
  err = handler.sqlpaxos.CommitTxn(&args, &reply)

  if err != nil {
    fmt.Println("Error :", err)
    return err
  }
  return nil
}

func (handler *Handler) RollbackTxn(con *barista.Connection, txn *sql.Tx) error {
  clientid := *(con.ClientId)
  requestid := *(con.SeqId)

  client_id, err := strconv.ParseInt(clientid, 10, 64)
  if err != nil {
    fmt.Println("Error: ", err)
  }

  request_id, err := strconv.Atoi(requestid)
  if err != nil {
    fmt.Println("Error: ", err)
  }
  args := sqlpaxos.RollbackTxnArgs{ClientId: client_id, RequestId: request_id, Txn: txn}
  var reply sqlpaxos.RollbackTxnReply
  err = handler.sqlpaxos.RollbackTxn(&args, &reply)

  if err != nil {
    fmt.Println("Error :", err)
    return err
  }
  return nil
}

func (handler *Handler) CloseConnection(
    con *barista.Connection) (error) {
  clientid := *(con.ClientId)
  requestid := *(con.SeqId)

  client_id, err := strconv.ParseInt(clientid, 10, 64)
  if err != nil {
    fmt.Println("Error: ", err)
  }

  request_id, err := strconv.Atoi(requestid)
  if err != nil {
    fmt.Println("Error: ", err)
  }
  args := sqlpaxos.CloseArgs{ClientId: client_id, RequestId: request_id}
  var reply sqlpaxos.CloseReply
  return handler.sqlpaxos.Close(&args, &reply)
}