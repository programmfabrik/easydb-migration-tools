package cmd

import (
	"fmt"

	"github.com/programmfabrik/golib"
	"github.com/programmfabrik/sqlpro"

	_ "embed"
)

//go:embed initdb.sql
var schemaSQL string

func openDb(driver, dsn string) (db *sqlpro.DB, err error) {
	func() {
		if db != nil && err != nil {
			db.Close()
		}
		if err != nil {
			if db != nil {
				db.Close()
			}
			err = fmt.Errorf("unable to openDb %q: %w", dsn, err)
		}
	}()
	db, err = sqlpro.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	err = db.DB().Ping()
	if err != nil {
		return nil, fmt.Errorf("unable to ping db: %w", err)
	}
	golib.Pln("connected to db %q", dsn)

	schemaSQL = replaceSerial(string(db.Driver), schemaSQL)
	err = db.Exec(schemaSQL)
	if err != nil {
		return nil, err
	}
	return db, nil
}
