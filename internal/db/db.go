package db

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

var db *sql.DB

func Start(conn string) error {
	var err error
	db, err = sql.Open("postgres", conn)
	if err != nil {
		return err
	}
	return nil
}

func Stop() error {
	return db.Close()
}

func RefreshMatViews() error {
	rowCount, err := db.Query(`
select count(*)
from pg_matviews;`)
	if err != nil {
		return err
	}
	count := 0
	if rowCount.Next() {
		if err = rowCount.Scan(&count); err != nil {
			return nil
		}
	}

	rows, err := db.Query(`
select '"' || schemaname || '"."' || matviewname || '"'
from pg_matviews
order by schemaname, matviewname;`)
	if err != nil {
		return err
	}

	var view string
	var start, stop time.Time

	log.Printf("=====================================")
	log.Printf("	refreshing matviews started")
	for i := 1; rows.Next(); i++ {
		if err = rows.Scan(&view); err != nil {
			return err
		}
		start = time.Now()
		_, err = db.Exec(fmt.Sprintf("refresh materialized view %s;", view))
		if err != nil {
			log.Printf("failed to refresh: %s", view)
		}
		stop = time.Now()
		log.Printf("%04d of %04d => %s: %s", i, count, view, stop.Sub(start))
	}
	log.Printf("	refreshing matviews stoped")
	log.Printf("=====================================")

	return nil
}

func Reindex(excludedSchemas string) error {
	rowCount, err := db.Query(`
select count(*)
from pg_indexes
where schemaname not in ($1);`, excludedSchemas)
	if err != nil {
		return err
	}
	count := 0
	if rowCount.Next() {
		if err = rowCount.Scan(&count); err != nil {
			return err
		}
	}
	rows, err := db.Query(`
select '"' || schemaname || '"."' || indexname || '"'
from pg_indexes
where schemaname not in ($1)
order by schemaname, indexname;`, excludedSchemas)
	if err != nil {
		return nil
	}

	var index string
	var start, stop time.Time

	log.Printf("=====================================")
	log.Printf("		  reindex started")
	for i := 1; rows.Next(); i++ {
		if err = rows.Scan(&index); err != nil {
			return err
		}
		start = time.Now()
		_, err = db.Exec(fmt.Sprintf("reindex index %s;", index))
		if err != nil {
			log.Printf("failed to reindex: %s", index)
		}
		stop = time.Now()
		log.Printf("%04d of %04d => %s: %s", i, count, index, stop.Sub(start))
	}
	log.Printf("		  reindex stoped")
	log.Printf("=====================================")

	return nil
}

func Vacuum(excludedSchemas string) error {
	countRows, err := db.Query(`
select count(*)
from information_schema.tables
where table_schema not in ($1)        
	and table_type = 'BASE TABLE';`, excludedSchemas)
	if err != nil {
		return err
	}
	count := 0
	if countRows.Next() {
		if err = countRows.Scan(&count); err != nil {
			return err
		}
	}
	rows, err := db.Query(`
select ('"' || table_schema || '"."' || table_name || '"')
from information_schema.tables
where table_schema not in ($1)        
	and table_type = 'BASE TABLE'
order by table_schema, table_name;`, excludedSchemas)
	if err != nil {
		return err
	}

	var table string
	var start, stop time.Time

	log.Printf("=====================================")
	log.Printf("		  vacuum started")
	for i := 1; rows.Next(); i++ {
		if err = rows.Scan(&table); err != nil {
			return err
		}
		start = time.Now()
		_, err = db.Exec(fmt.Sprintf("vacuum analyze %s", table))
		if err != nil {
			log.Printf("failed to vacuum table: %s", table)
		}
		stop = time.Now()
		log.Printf("%04d of %04d => %s: %s", i, count, table, stop.Sub(start))
	}
	log.Printf("		  vacuum stoped")
	log.Printf("=====================================")

	return nil
}

func ExecFromFile(file string) error {
	fo, err := os.Open(file)
	if err != nil {
		return err
	}
	defer fo.Close()

	scanner := bufio.NewScanner(fo)
	for scanner.Scan() {
		sqlQuery := scanner.Text()
		log.Printf("=====================================")
		log.Printf("running file started: %s", file)
		_, err = db.Exec(sqlQuery)
		if err != nil {
			return err
		}
		log.Printf("running file stoped: %s", file)
		log.Printf("=====================================")
	}

	return nil
}
