package main

import (
	"github.com/alecthomas/kong"
	"github.com/programmfabrik/easydb-migration-tools/k10plus/cmd"
)

var cli struct {
	Convert cmd.Convert `cmd:"" help:"convert a k10plus files to sqlite"`
}

func main() {
	ctx := kong.Parse(&cli, kong.Vars{}, kong.Description(`Handle k10plus files swiftly`))

	err := ctx.Run()
	ctx.FatalIfErrorf(err)
}
