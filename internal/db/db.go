package db

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"time"
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
		rowCount.Scan(&count)
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
			return err
		}
		stop = time.Now()
		log.Printf("%04d of %04d => %s: %s", i, count, view, stop.Sub(start))
	}
	log.Printf("	refreshing matviews stoped")
	log.Printf("=====================================")

	return nil
}
