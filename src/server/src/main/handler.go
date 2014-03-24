package main

/**
 * Barista Handler
 *
 * @author: Anant Bhardwaj
 * @date: 03/23/2014
 */

import "barista"
import "db"
import "reflect"

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
  
  user := *(con_params.User)
  password := *(con_params.Password)
  database := *(con_params.Database)

  handler.manager.Connect(user, password, database)
  con := new(barista.Connection)
  con.User = &user
  con.Database = &database
  return con, nil
}

func (handler *Handler) ExecuteSql(con *barista.Connection,
    query string, query_params [][]byte) (*barista.ResultSet, error) {
  
  rows, columns, _ := handler.manager.ExecuteSql(query, query_params)
  tuples := []*barista.Tuple{}
  for _, row := range rows {
    cells := []*barista.Cell{}
    vals := reflect.ValueOf(row)
    for i:=0; i < vals.Len(); i++ {
      val := vals.Index(i).Interface().([]byte)
      cell := barista.Cell{Value: &val}
      cells = append(cells, &cell)
    }
    tuple := barista.Tuple{Cells: &cells}
    tuples = append(tuples, &tuple)
  }
 
  result_set := new(barista.ResultSet)
  result_set.Con = con
  result_set.Tuples = &tuples
  result_set.FieldNames = &columns

  return result_set, nil
}