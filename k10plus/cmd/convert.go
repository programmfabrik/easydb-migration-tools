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
	"time"

	"github.com/alecthomas/kong"
	_ "github.com/mattn/go-sqlite3"
	"github.com/programmfabrik/golib"
	"github.com/programmfabrik/sqlpro"

	_ "embed"
)

//go:embed initdb.sql
var schemaSQL string

type Convert struct {
	SourceDir   string `help:"Directory containing the pica files"`
	Sqlite      string `help:"Path to sqlite file"`
	MaxParallel int    `help:"Number of concurrent workers to deploy. 0 uses the number of available CPUs." default:"1"`
}

func (c *Convert) Run(kctx *kong.Context) (err error) {
	db, err := sqlpro.Open(sqlpro.SQLITE3, c.Sqlite)
	if err != nil {
		return err
	}
	err = db.DB().Ping()
	if err != nil {
		return fmt.Errorf("unable to ping db: %w", err)
	}
	golib.Pln("connected to sqlite %q", c.Sqlite)

	ctx := context.Background()

	err = db.ExecContext(ctx, schemaSQL)
	if err != nil {
		return err
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

const (
	DELIMITER_OBJ = "\u001D" // Group separator (Information Separator Three)
	DELIMITER_REC = '\u001E' // Record separator (Information Separator Two)
	DELIMITER_VAL = "\u001F" //  Unit separator (Information Separator One)
)

type file struct {
	ID        int    `db:"id,pk,omitempty"`
	Filepath  string `db:"filepath"`
	itemCount int
}

type item struct {
	ID     int `db:"id,pk,omitempty"`
	FileID int `db:"file_id"`
	values []*value
}

type value struct {
	ID        int    `db:"id,pk,omitempty"`
	ItemID    int    `db:"item_id"`
	Feld      string `db:"feld"`
	Unterfeld string `db:"unterfeld"`
	Wert      string `db:"wert"`
}

func (c *Convert) Import(ctx context.Context, db *sqlpro.DB, filepath string) error {

	start := time.Now()

	// add file to database
	f := file{}
	err := db.QueryContext(ctx, &f, `SELECT * FROM "file" WHERE "filepath" = ?`, filepath)
	if err != nil {
		if err != sqlpro.ErrQueryReturnedZeroRows {
			return err
		}
		f.Filepath = filepath
		err = db.InsertContext(ctx, "file", &f)
		if err != nil {
			return err
		}
	}

	// Open the file for reading.
	file, err := os.Open(filepath)
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
				err = itm.save(ctx, db, &f)
				if err != nil {
					return err
				}
			}
			itm = &item{
				FileID: f.ID,
				values: []*value{},
			}
		case len(line) > 0 && line[0] == DELIMITER_REC:
			parts := strings.Split(line[1:], DELIMITER_VAL)
			// 0 : feld
			// 1 buchstabe: unterfeld
			// rest: wert
			for _, val := range parts[1:] {
				itm.values = append(itm.values, &value{
					Feld:      parts[0],
					Unterfeld: val[0:1],
					Wert:      val[1:],
				})
			}
		}
	}
	// Check if the scanner encountered any error.
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("Error reading file: %w", err)
	}
	if itm != nil {
		err = itm.save(ctx, db, &f)
		if err != nil {
			return err
		}
	}
	golib.Pln("imported %q with %d items in %s", f.Filepath, f.itemCount, time.Since(start))

	return nil
}

func (itm item) save(ctx context.Context, db *sqlpro.DB, f *file) (err error) {
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
	err = tx.InsertContext(ctx, "item", &itm)
	if err != nil {
		return err
	}
	for _, v := range itm.values {
		v.ItemID = itm.ID
	}
	err = tx.InsertBulkContext(ctx, "value", itm.values)
	if err != nil {
		return err
	}
	f.itemCount++
	// golib.Pln("item %d -- %d values", itm.ID, len(itm.values))
	return nil
}
