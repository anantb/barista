package main

/**
 * Barista Handler
 *
 * @author: Anant Bhardwaj
 * @date: 03/23/2014
 */

import "barista"
import "db"

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

func (handler *Handler) Connect(
    con_params *barista.ConnectionParams) (*barista.Connection, error) {
  
  handler.manager.Connect(*con_params.User, *con_params.Password, *con_params.Database)
  con := new(barista.Connection)
  *con.User = *con_params.User
  *con.Database = *con_params.Database
  return con, nil
}

func (handler *Handler) ExecuteSql(con *barista.Connection,
    query string, query_params [][]byte) (*barista.ResultSet, error) {
  
  result_set := new(barista.ResultSet)
  result_set.Con = con

  return result_set, nil
}