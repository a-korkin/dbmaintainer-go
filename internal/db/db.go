package db

import (
	"database/sql"
	_ "github.com/lib/pq"
)

var Db *sql.DB

func Start(conn string) error {
	var err error
	Db, err = sql.Open("postgres", conn)
	if err != nil {
		return err
	}
	return nil
}

func Stop() error {
	return Db.Close()
}
