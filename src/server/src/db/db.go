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
    query string, args interface{}) ([]string, error) { 

  rows, err := manager.db.Query(query, args)
  column_names, _ := rows.Columns()
  return column_names, err
}