package storage

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type PostgresStorageManager struct {
	connections map[string]*sql.Tx
	db          *sql.DB
	dataTable   string
}

const (
	connTemplate = "host=%s user=%s password=%s dbname=%s"
	putTemplate  = "INSERT INTO %1 VALUES (%2, %3);"
	getTemplate  = "SELECT val FROM %1 WHERE key=%2;"
)

func NewPostgresStorageManager(user string,
	password string,
	address string,
	dbname string,
	table string) (*PostgresStorageManager, error) {
	connStr := fmt.Sprintf(connTemplate, address, user, password, dbname)

	db, err := sql.Open("postgres", connStr)

	if err != nil {
		return nil, err
	}

	return &PostgresStorageManager{
		db:          db,
		connections: map[string]*sql.Tx{},
		dataTable:   table,
	}, nil
}

func (pg *PostgresStorageManager) StartTransaction(id string) bool {
	tx, err := pg.db.Begin()
	if err != nil {
		return false
	}

	pg.connections[id] = tx
	return true
}

func (pg *PostgresStorageManager) CommitTransaction(id string) bool {
	tx := pg.connections[id]
	err := tx.Commit()

	delete(pg.connections, id)
	return err == nil
}

func (pg *PostgresStorageManager) AbortTransaction(id string) bool {
	tx := pg.connections[id]
	err := tx.Rollback()

	delete(pg.connections, id)
	return err == nil
}

func (pg *PostgresStorageManager) Put(key string, val []byte, tid string) bool {
	tx := pg.connections[tid]
	_, err := tx.Exec(putTemplate, pg.dataTable, key, val)

	return err == nil
}

func (pg *PostgresStorageManager) Get(key string, tid string) ([]byte, error) {
	tx := pg.connections[tid]
	result := tx.QueryRow(getTemplate, pg.dataTable, key)

	var output []byte
	err := result.Scan(output)

	if err != nil {
		return nil, err
	}

	return output, nil
}
