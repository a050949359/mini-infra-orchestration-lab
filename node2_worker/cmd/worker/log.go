package main

import (
	"log"
	"os"
	"path/filepath"
)

func setupLogOutput(path string) func() {
	if path == "" {
		return func() {}
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		log.Printf("unable to create log directory for %s: %v", path, err)
		return func() {}
	}

	logFile, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("unable to open log file %s: %v", path, err)
		return func() {}
	}

	log.SetOutput(logFile)
	log.Printf("file logging enabled: %s", path)

	return func() {
		if err := logFile.Close(); err != nil {
			log.Printf("close log file failed: %v", err)
		}
	}
}
