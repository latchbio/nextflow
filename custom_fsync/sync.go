package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

func main() {
	if len(os.Args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s\n", os.Args[0])
		os.Exit(1)
	}

	err := filepath.WalkDir(".", func(path string, di fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		f, err := os.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file: %s: %v\n", path, err)
			return err
		}

		defer f.Close()

		if err := f.Sync(); err != nil {
			fmt.Fprintf(os.Stderr, "Error fsyncing file: %s: %v\n", path, err)
			return err
		}

		return nil
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error walking directory: %v\n", err)
		os.Exit(1)
	}
}
