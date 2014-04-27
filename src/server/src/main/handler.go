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

type Handler struct {
  manager *db.DBManager 
}

func NewBaristaHandler() *Handler {
  handler := new(Handler)
  handler.manager = new(db.DBManager)
  return handler
}

func (handler *Handler) GetVersion() (float64, error) {
  return barista.VERSION, nil
}

func (handler *Handler) OpenConnection(
    con_params *barista.ConnectionParams) (*barista.Connection, error) {
  
  user := con_params.User
  password := con_params.Password
  database := con_params.Database

  err := handler.manager.OpenConnection(user, password, database)

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
  
  rows, columns, err := handler.manager.ExecuteSql(query, query_params)

  if err != nil {
    fmt.Println("Error :", err)
    return nil, err
  }

  tuples := []*barista.Tuple{}
  for _, row := range rows {
    tuple := barista.Tuple{Cells: row}
    tuples = append(tuples, &tuple)
  }
 
  result_set := new(barista.ResultSet)
  result_set.Con = con
  result_set.Tuples = tuples
  result_set.FieldNames = columns

  return result_set, nil
}

func (handler *Handler) CloseConnection(
    con *barista.Connection) (error) {
  return handler.manager.CloseConnection()
}