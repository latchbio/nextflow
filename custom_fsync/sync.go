package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s\n", os.Args[0])
		os.Exit(1)
	}

	files, err := os.ReadDir(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading directory: %v\n", err)
		os.Exit(1)
	}

	for _, file := range files {
		filePath := file.Name()
		if file.IsDir() {
			continue
		}

		f, err := os.OpenFile(filePath, os.O_RDONLY, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file: %s: %v\n", filePath, err)
			continue
		}
		defer f.Close()

		if err := f.Sync(); err != nil {
			fmt.Fprintf(os.Stderr, "Error fsyncing file: %s: %v\n", filePath, err)
			continue
		}
	}
}
