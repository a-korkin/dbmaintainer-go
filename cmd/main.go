package main

import (
	"github.com/a-korkin/db_maintenancer/configs"
	"github.com/a-korkin/db_maintenancer/internal/db"
	"log"
	"time"
)

func main() {
	conn, err := configs.GetEnv("DB_CONNECTION")
	if err != nil {
		log.Fatalf("failed to get DB_CONNECTION: %s", err)
	}

	err = db.Start(conn)
	if err != nil {
		log.Fatalf("failed to connect to db: %s", err)
	}
	log.Printf("everything is ok")
	time.Sleep(2 * time.Second)
	defer func() {
		log.Printf("closing connection")
		if err = db.Stop(); err != nil {
			log.Fatalf("failed to close connection to db: %s", err)
		}
	}()
}
