package main

/**
 * Barista Handler
 *
 * @author: Anant Bhardwaj
 * @date: 03/23/2014
 *
 */

import (
  "barista"
)

type BaristaHandler struct {
}

func NewBaristaHandler() *BaristaHandler {
  return &BaristaHandler{}
}

func (handler *BaristaHandler) GetVersion() (float64, error) {

  return barista.VERSION, nil
}

func (handler *BaristaHandler) Connect(
    con_params *barista.ConnectionParams) (*barista.Connection, error) {

  return &barista.Connection{}, nil
}

func (handler *BaristaHandler) ExecuteSql(con *barista.Connection,
    query string, query_params [][]byte) (*barista.ResultSet, error) {

  return &barista.ResultSet{}, nil
}