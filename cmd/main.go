package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/a-korkin/db_maintenancer/configs"
	"github.com/a-korkin/db_maintenancer/internal/db"
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

func startScripts() {
	scrp, err := configs.GetEnv("SCRIPTS_RUN")
	if err != nil {
		log.Fatalf("failed to get SCRIPTS_RUN: %s", err)
	}
	needScripts, err := strconv.ParseBool(scrp)
	if err != nil {
		log.Fatalf("failed to parse: %s", err)
	}
	scriptsDir, err := configs.GetEnv("SCRIPTS_PATH")
	if err != nil {
		log.Fatalf("failed to get SCRIPTS_PATH: %s", err)
	}
	if !needScripts {
		return
	}
	entries, err := os.ReadDir(scriptsDir)
	for _, f := range entries {
		filePath := filepath.Join(scriptsDir, f.Name())
		if err = db.ExecFromFile(filePath); err != nil {
			log.Fatalf("failed to exec scripts from files: %s", err)
		}
	}
}

func setLogs() {
	logsPath, err := configs.GetEnv("LOGS_PATH")
	if err != nil {
		log.Fatalf("failed to get LOGS_PATH: %s", err)
	}
	today := time.Now()
	logFile := fmt.Sprintf("%d_%02d_%02d.txt", today.Year(), today.Month(), today.Day())
	_, err = os.Stat(logsPath)
	if err != nil {
		if err = os.Mkdir(logsPath, os.ModePerm); err != nil {
			log.Fatalf("failed to create logging directory: %s", err)
		}
	}
	file, err := os.OpenFile(
		filepath.Join(logsPath, logFile),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	multi := io.MultiWriter(file, os.Stdout)
	if err == nil {
		log.SetOutput(multi)
	} else {
		log.Printf("failed to open log file: %s", err)
	}
}

func main() {
	setLogs()
	conn, err := configs.GetEnv("DB_CONNECTION")
	if err != nil {
		log.Fatalf("failed to get DB_CONNECTION: %s", err)
	}
	excludedSchemas, err := configs.GetEnv("EXCLUDED_SCHEMAS")
	if err != nil {
		log.Printf("failed to get EXCLUDED_SCHEMAS: %s", err)
	}
	excluded := fmt.Sprintf("'%s'", strings.Replace(excludedSchemas, ",", "','", -1))

	err = db.Start(conn)
	if err != nil {
		log.Fatalf("failed to connect to db: %s", err)
	}

	startRefresh()
	startVacuum(excluded)
	startReindex(excluded)
	startScripts()

	defer func() {
		if err = db.Stop(); err != nil {
			log.Fatalf("failed to close connection to db: %s", err)
		}
	}()
}
