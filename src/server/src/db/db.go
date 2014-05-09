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
const PORT = "5432"

type DBManager struct {
  db *sql.DB
  Port string
}

func NewDBManager() *DBManager {
  manager := new(DBManager)
  manager.Port = PORT
  return manager
}

func (manager *DBManager) OpenConnection(
    user string, password string, dbname string) error {

  var err error
  manager.db, err = sql.Open(
    "postgres",
    fmt.Sprintf(
        "host=%s port=%v user=%s dbname=%s password=%s sslmode=disable",
        HOST, manager.db.Port, user, dbname, password))

  return err
}

func (manager *DBManager) BeginTxn() (*sql.Tx, error) {
  return manager.db.Begin()
}

func (manager *DBManager) CommitTxn(tx *sql.Tx) error {
  return tx.Commit()
}

func (manager *DBManager) RollbackTxn(tx *sql.Tx) error {
  return tx.Rollback()
}

func (manager *DBManager) QueryTxn(
    tx *sql.Tx, query string, args ...interface{}) ([][][]byte, []string, error) {

  rows, err := tx.Query(query, args...)

  if err != nil {
    return nil, nil, err
  }

  return manager.formatRows(rows)
}

func (manager *DBManager) ExecTxn(tx *sql.Tx, query string, args ...interface{}) (sql.Result, error) {
  return tx.Exec(query, args...)
}

func (manager *DBManager) ExecuteSql(
    query string, args ...interface{}) ([][][]byte, []string, error) { 

  rows, err := manager.db.Query(query, args...)
  
  if err != nil {
    return nil, nil, err
  }

  return manager.formatRows(rows)
}

func (manager *DBManager) formatRows(rows *sql.Rows) ([][][]byte, []string, error) {

  tuples := make([][][]byte, 0)

  columns, err := rows.Columns()
  n_columns := len(columns)

  for rows.Next() {
     cells := make([][]byte, n_columns)

     dest := make([]interface{}, n_columns)
     for i, _ := range cells {
        dest[i] = &cells[i]
     }

     rows.Scan(dest...)
     tuples = append(tuples, cells)
  }

  return tuples, columns, err
}

func (manager *DBManager) CloseConnection() error {
  return manager.db.Close()
}