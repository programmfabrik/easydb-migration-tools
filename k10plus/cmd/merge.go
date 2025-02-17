package cmd

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/programmfabrik/golib"
	"github.com/programmfabrik/sqlpro"
)

type Merge struct {
	SourceDir string `help:"Directory containing the k10 sqlite files"`
	Target    string `help:"Target sqlite where all files from source dir should me merged into."`
	targetDb  *sqlpro.DB
}

func (m *Merge) Run(kctx *kong.Context) (err error) {
	m.targetDb, err = openDb(sqlpro.SQLITE3, m.Target)
	if err != nil {
		return fmt.Errorf("unable to open db file %q: %w", m.Target, err)
	}
	defer m.targetDb.Close()
	ctx := context.Background()
	fsys := os.DirFS(m.SourceDir)
	err = fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Fatal(err)
		}
		if !strings.HasSuffix(path, ".sqlite") {
			return nil
		}
		if m.Target == path {
			return nil
		}
		err = m.Append(ctx, filepath.Join(m.SourceDir, path))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *Merge) Append(ctx context.Context, path string) (err error) {
	golib.Pln("merging %q...", path)
	start := time.Now()
	err = m.targetDb.ExecContext(ctx, `
	ATTACH @ AS other;
	INSERT INTO "item" SELECT * FROM other."item";
	INSERT INTO "value" SELECT * FROM other."value";
	DETACH other;
	`, path)
	if err != nil {
		return err
	}
	golib.Pln("merged %q, took %s", path, time.Since(start))

	return nil
}
