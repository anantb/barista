package db

/**
 * Barista DB Manager
 *
 * @author: Anant Bhardwaj
 * @date: 03/23/2014
 */

import (
  "database/sql"
  "fmt"
)

type DBManager struct {
  db *sql.DB
}

func NewDBManager() *DBManager {
  manager := new(DBManager)
  return manager
}

func (manager * DBManager) Connect(
    user string, password string, dbname string) error {

  var err error
  manager.db, err = sql.Open(
    "postgres",
    fmt.Sprintf("user=%s dbname=%s password=%s", user, password, dbname))

  return err
}

func (manager *DBManager) ExecuteSql(
    query string, args interface{}) ([]interface{}, []string, error) { 

  rows, err := manager.db.Query(query, args)
  columns, _ := rows.Columns()
  n_columns := len(columns)
  tuples := []interface{}{}
  for rows.Next() {
    cells := make([]sql.RawBytes, n_columns)
    rows.Scan(cells)
    tuples = append(tuples, cells)
  }
  return tuples, columns, err
}