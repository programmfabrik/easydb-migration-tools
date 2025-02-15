package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kong"
	_ "github.com/mattn/go-sqlite3"
	"github.com/programmfabrik/golib"
	"github.com/programmfabrik/sqlpro"

	_ "embed"
)

const K10_ID_FELD string = "003@"
const K10_ID_FELD_UNTERFELD string = "003@0"

//go:embed initdb.sql
var schemaSQL string

const (
	DELIMITER_OBJ = "\u001D" // Group separator (Information Separator Three)
	DELIMITER_REC = '\u001E' // Record separator (Information Separator Two)
	DELIMITER_VAL = "\u001F" //  Unit separator (Information Separator One)
)

type item struct {
	File   string `db:"file"`
	ItemID string `db:"item_id"`
	values []*value
}

type file struct {
	Filepath  string
	itemCount int
}

type value struct {
	ItemID    string `db:"item_id"`
	Feld      string `db:"feld"`
	Unterfeld string `db:"unterfeld"`
	Idx       int    `db:"idx"`
	Wert      string `db:"wert"`
}

type Convert struct {
	SourceDir   string `help:"Directory containing the pica files"`
	DSN         string `help:"DSN to connect to database. Use sqlite3:<file.sqlite> or postgres:host=localhost port=5432 dbname=apitest password=egal sslmode=disable to connect."`
	MaxParallel int    `help:"Number of concurrent workers to deploy. 0 uses the number of available CPUs." default:"1"`
	// Records     bool   `help:"If set, create k10_record table with colunns for each feld+unterfeld. This is not supported for --single-files mode." default:"true"`
	DryRun      bool   `help:"Only load payloads and output some timings."`
	SingleFiles string `help:"Directory to put single sqlite files. One for each import payload. Fastest way to concurrently load data."`
	columns     map[string]bool
	lck         sync.Mutex
}

func (c *Convert) openDb(driver, dsn string) (db *sqlpro.DB, err error) {
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

func (c *Convert) Run(kctx *kong.Context) (err error) {

	ctx := context.Background()
	var db *sqlpro.DB
	if c.SingleFiles == "" {
		driver, dsn, found := strings.Cut(c.DSN, ":")
		if !found {
			return fmt.Errorf("dns %q malformed", c.DSN)
		}
		db, err = c.openDb(driver, dsn)
		if err != nil {
			return err
		}
		defer db.Close()

		// check column names (needed for record mode)
		c.columns = map[string]bool{}
		tbs, err := getTables(db)
		if err != nil {
			return err
		}
		for _, tb := range tbs {
			if !strings.HasPrefix(tb, "k10_record") {
				continue
			}
			cols, err := getColumns(db, tb)
			if err != nil {
				return err
			}
			for _, col := range cols {
				c.columns[col] = true
			}
		}
	} else {
		err = os.MkdirAll(c.SingleFiles, 0755)
		if err != nil {
			return err
		}
	}

	cm := golib.ConcurrentManager(c.MaxParallel)
	golib.Pln("scanning %q for .pp files with %d workers", c.SourceDir, cm.Workers())

	fsys := os.DirFS(c.SourceDir)
	err = fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Fatal(err)
		}
		if !strings.HasSuffix(path, ".pp") {
			return nil
		}
		func(path string) {
			cm.Run(func(runId int) error {
				err = c.Import(ctx, db, filepath.Join(c.SourceDir, path))
				if err != nil {
					return err
				}
				return nil
			})
		}(path)
		if err != nil {
			return err
		}
		if len(cm.Errors()) > 0 {
			return fmt.Errorf("have errors: %v", cm.Errors())
		}
		return nil
	})
	if err != nil {
		return err
	}
	err = cm.Wait()
	if err != nil {
		return err
	}
	return nil
}

func replaceSerial(driver, schemaSQL string) string {
	switch driver {
	case sqlpro.SQLITE3:
		schemaSQL = strings.ReplaceAll(schemaSQL, "--SERIAL--", `"id" INTEGER PRIMARY KEY AUTOINCREMENT,`)
	case sqlpro.POSTGRES:
		schemaSQL = strings.ReplaceAll(schemaSQL, "--SERIAL--", `"id" SERIAL PRIMARY KEY,`)
	}
	return schemaSQL
}

func (c *Convert) Import(ctx context.Context, db *sqlpro.DB, fp string) (err error) {

	if c.SingleFiles != "" {
		dbFile := filepath.Base(fp) + ".sqlite"
		dbFilepath := filepath.Join(c.SingleFiles, dbFile)
		_, err := os.Stat(dbFilepath)
		if err == nil {
			// file exists, check if we have a "-journal" file in which case we continue
			_, err = os.Stat(dbFilepath + "-journal")
			if err == nil {
				golib.Pln("%q-journal exists, removing .sqlite file and redo", dbFilepath)
				os.Remove(dbFilepath)
			} else {
				// no -journal file, assume .pp file is imported
				golib.Pln("%q exists, skipping", dbFilepath)
				return nil
			}
		}

		db, err = c.openDb(sqlpro.SQLITE3, dbFilepath)
		if err != nil {
			return fmt.Errorf("unable to open db file %q: %w", dbFilepath, err)
		}
		defer db.Close()
		golib.Pln("importing %q into %q...", fp, dbFilepath)
	} else {
		golib.Pln("importing %q...", fp)
	}
	start := time.Now()

	defer func() {
		if err != nil {
			golib.Pln("error: %s", err.Error())
		}
	}()

	f := file{
		Filepath: fp,
	}

	// Open the file for reading.
	file, err := os.Open(fp)
	if err != nil {
		return fmt.Errorf("Error opening file: %w", err)
	}
	// Ensure the file is closed when we're done.
	defer file.Close()

	// Create a new Scanner for the file.
	scanner := bufio.NewScanner(file)

	// Iterate over each line in the file.
	var itm *item
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		switch {
		case line == DELIMITER_OBJ:
			if itm != nil {
				err = c.save(ctx, db, itm, &f)
				if err != nil {
					return err
				}
			}
			itm = &item{
				File:   filepath.Base(f.Filepath),
				values: []*value{},
			}
		case len(line) > 0 && line[0] == DELIMITER_REC:
			parts := strings.Split(line[1:], DELIMITER_VAL)
			// 0 : feld
			// 1 buchstabe: unterfeld
			// rest: wert
			feld := strings.TrimSpace(parts[0])
			for idx, val := range parts[1:] {
				if len(val) < 2 {
					// ignore empty feld value
					continue
				}
				itm.values = append(itm.values, &value{
					Feld:      feld,
					Unterfeld: val[0:1],
					Wert:      val[1:],
					Idx:       idx,
				})
			}
			if feld == K10_ID_FELD {
				itm.ItemID = itm.values[len(itm.values)-1].Wert
			}
		}
	}
	// Check if the scanner encountered any error.
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("Error reading file: %w", err)
	}
	if itm != nil {
		err = c.save(ctx, db, itm, &f)
		if err != nil {
			return err
		}
	}
	golib.Pln("imported %q with %d items in %s", f.Filepath, f.itemCount, time.Since(start))

	return nil
}

func (c *Convert) save(ctx context.Context, db *sqlpro.DB, itm *item, f *file) (err error) {
	if c.DryRun {
		f.itemCount++
		return nil
	}
	// if c.Records {
	// 	err = c.saveRecord(ctx, db, itm, f)
	// } else {
	err = c.saveValues(ctx, db, itm, f)
	// }
	if err != nil {
		return err
	}
	if f.itemCount%10_000 == 0 {
		golib.Pln("%s: %d", f.Filepath, f.itemCount)
	}
	return nil
}

func (c *Convert) saveValues(ctx context.Context, db *sqlpro.DB, itm *item, f *file) (err error) {

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	err = tx.InsertContext(ctx, "item", itm)
	if err != nil {
		return fmt.Errorf("unable to insert k10_item: %w", err)
	}
	for _, v := range itm.values {
		v.ItemID = itm.ItemID
	}
	err = tx.InsertBulkContext(ctx, "value", itm.values)
	if err != nil {
		return err
	}
	f.itemCount++
	// golib.Pln("item %d -- %d values", itm.ID, len(itm.values))
	return nil
}

// func (c *Convert) saveRecord(ctx context.Context, db *sqlpro.DB, itm *item, f *file) (err error) {
// 	rec := map[string]string{}
// 	cols := map[string]bool{}
// 	for _, v := range itm.values {
// 		col := v.Feld + v.Unterfeld
// 		if !cols[col] && len(cols) < 1580 {
// 			cols[col] = true
// 		}
// 		if rec[col] != "" {
// 			rec[col] += ";"
// 		}
// 		rec[col] += v.Wert
// 	}

// 	// make sure all columns exist
// 	for col := range cols {
// 		_, ok := c.columns[col]
// 		if !ok {
// 			c.lck.Lock()
// 			appendix := col[0:2] // first 0 field names
// 			err = createRecordTable(ctx, db, appendix)
// 			if err != nil {
// 				c.lck.Unlock()
// 				return err
// 			}
// 			err = db.ExecContext(ctx, `ALTER TABLE "k10_record_`+appendix+`"
// 				ADD COLUMN IF NOT EXISTS `+db.Esc(col)+` TEXT`)
// 			if err != nil {
// 				c.lck.Unlock()
// 				return err
// 			}
// 			c.lck.Unlock()
// 			c.columns[col] = true
// 			golib.Pln(`added "k10_record_%s".%q`, appendix, col)
// 		}
// 	}
// 	sql := map[string]string{} // map[appendix]sqlCmds
// 	sqlValues := map[string]string{}
// 	for col := range cols {
// 		appendix := col[0:2]
// 		_, ok := sql[appendix]
// 		if !ok {
// 			sql[appendix] = `INSERT INTO "k10_record_` + appendix + `"("file_id","identifier"`
// 			sqlValues[appendix] = ""
// 		}
// 		sql[appendix] += `,` + db.Esc(col)
// 		sqlValues[appendix] += `,` + db.EscValue(rec[col])
// 	}
// 	idf := rec[K10_ID_FELD_UNTERFELD]
// 	if idf == "" {
// 		idf = "NULL"
// 	} else {
// 		idf = db.EscValue(idf)
// 	}
// 	for app, sqlS := range sql {
// 		sqlS += ") VALUES (" + strconv.Itoa(f.ID) + "," + idf + sqlValues[app] + ")"
// 		err = db.ExecContext(ctx, sqlS)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	f.itemCount++
// 	return nil
// }

// func createRecordTable(ctx context.Context, db *sqlpro.DB, appendix string) error {
// 	schemaSQL := fmt.Sprintf(`
// 	CREATE TABLE IF NOT EXISTS "k10_record_%s" (
// 		--SERIAL--
// 		"identifier" TEXT UNIQUE,
// 		"file_id"   INTEGER NOT NULL,
// 		FOREIGN KEY("file_id") REFERENCES "k10_file"("id")
// 	);
// 	`, appendix)
// 	return db.ExecContext(ctx, replaceSerial(string(db.Driver), schemaSQL))
// }

// getColumns returns the list of column names for the given table.
func getColumns(db *sqlpro.DB, tableName string) ([]string, error) {
	// LIMIT 0 returns no rows but lets us inspect the columns.
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName)
	rows, err := db.DB().Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rows.Columns()
}

// getTables returns a list of table names depending on the driver.
func getTables(db *sqlpro.DB) ([]string, error) {
	var query string
	switch db.Driver {
	case sqlpro.SQLITE3:
		// Exclude internal SQLite tables
		query = `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';`
	case sqlpro.POSTGRES:
		// Retrieves tables from the public schema in PostgreSQL
		query = `SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';`
	default:
		return nil, fmt.Errorf("unsupported driver: %s", db.Driver)
	}
	tables := []string{}
	err := db.Query(&tables, query)
	if err != nil {
		return nil, err
	}
	return tables, nil
}
