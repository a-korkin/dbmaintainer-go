package main

import (
	"github.com/a-korkin/db_maintenancer/configs"
	"github.com/a-korkin/db_maintenancer/internal/db"
	"log"
	"strings"
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
	// if err = db.RefreshMatViews(); err != nil {
	// 	log.Fatalf("failed to refresh matviews: %s", err)
	// }
	excludedSchemas, err := configs.GetEnv("EXCLUDED_SCHEMAS")
	if err != nil {
		log.Printf("failed to get EXCLUDED_SCHEMAS: %s", err)
	}
	excludedSchemas = strings.Replace(excludedSchemas, ",", "','", -1)
	// if err = db.Reindex(excludedSchemas); err != nil {
	// 	log.Fatalf("failed to reindex: %s", err)
	// }
	if err = db.Vacuum(excludedSchemas); err != nil {
		log.Fatalf("failed to vacuum: %s", err)
	}
	defer func() {
		if err = db.Stop(); err != nil {
			log.Fatalf("failed to close connection to db: %s", err)
		}
	}()
}
