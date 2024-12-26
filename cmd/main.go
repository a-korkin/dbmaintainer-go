package main

import (
	"github.com/a-korkin/db_maintenancer/configs"
	"github.com/a-korkin/db_maintenancer/internal/db"
	"log"
	"strconv"
	"strings"
)

func startRefresh() {
	refresh, err := configs.GetEnv("REFRESH_MATVIEWS")
	if err != nil {
		log.Printf("failed to get REFRESH_MATVIEWS: %s", err)
	}
	needRefresh, err := strconv.ParseBool(refresh)
	if err != nil {
		log.Printf("failed to parse: %s", err)
	}
	if needRefresh {
		if err = db.RefreshMatViews(); err != nil {
			log.Fatalf("failed to refresh matviews: %s", err)
		}
	}
}

func startReindex(excluded string) {
	reindex, err := configs.GetEnv("REINDEX")
	if err != nil {
		log.Printf("failed to get REINDEX: %s", err)
	}
	needReindex, err := strconv.ParseBool(reindex)
	if err != nil {
		log.Printf("failed to parse: %s", err)
	}
	if needReindex {
		if err = db.Reindex(excluded); err != nil {
			log.Fatalf("failed to reindex: %s", err)
		}
	}
}

func startVacuum(excluded string) {
	vacuum, err := configs.GetEnv("VACUUM")
	if err != nil {
		log.Printf("failed to get VACUUM: %s", err)
	}
	needVacuum, err := strconv.ParseBool(vacuum)
	if err != nil {
		log.Printf("failed to parse: %s", err)
	}
	if needVacuum {
		if err = db.Vacuum(excluded); err != nil {
			log.Fatalf("failed to vacuum: %s", err)
		}
	}
}

func main() {
	conn, err := configs.GetEnv("DB_CONNECTION")
	if err != nil {
		log.Fatalf("failed to get DB_CONNECTION: %s", err)
	}
	excludedSchemas, err := configs.GetEnv("EXCLUDED_SCHEMAS")
	if err != nil {
		log.Printf("failed to get EXCLUDED_SCHEMAS: %s", err)
	}
	excluded := strings.Replace(excludedSchemas, ",", "','", -1)

	err = db.Start(conn)
	if err != nil {
		log.Fatalf("failed to connect to db: %s", err)
	}

	startRefresh()
	startVacuum(excluded)
	startReindex(excluded)

	defer func() {
		if err = db.Stop(); err != nil {
			log.Fatalf("failed to close connection to db: %s", err)
		}
	}()
}
