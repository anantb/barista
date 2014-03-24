package db

/**
 * Barista DB Manager
 *
 * @author: Anant Bhardwaj
 * @date: 03/23/2014
 */

import _ "github.com/lib/pq"
import "database/sql"
import "fmt"

const HOST = "localhost"
const PORT = 5432

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
    fmt.Sprintf("host=%s port=%v user=%s dbname=%s password=%s sslmode=disable", HOST, PORT, user, password, dbname))

  return err
}

func (manager *DBManager) ExecuteSql(
    query string, args interface{}) ([]interface{}, []string, error) { 

  rows, err := manager.db.Query(query)
  
  if err != nil {
    return nil, nil, err
  }

  columns, err := rows.Columns()
  n_columns := len(columns)
  tuples := []interface{}{}

  for rows.Next() {
    cells := make([][]byte, n_columns)
    rows.Scan(cells)
    tuples = append(tuples, cells)
  }

  return tuples, columns, err
}