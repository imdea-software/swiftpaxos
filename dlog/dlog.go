package dlog

import (
	"log"
	"os"
)

var DLOG = false

func Printf(format string, v ...any) {
	if DLOG {
		log.Printf(format, v...)
	}
}

func Println(v ...any) {
	if DLOG {
		log.Println(v...)
	}
}

type Logger struct {
	logger  *log.Logger
	verbose bool
}

func New(logPath string, verbose bool) *Logger {
	logF := os.Stdout
	if logPath != "" {
		var err error
		logF, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal("Can't open log file:", logPath)
		}
	}
	return &Logger{
		logger:  log.New(logF, "", log.LstdFlags),
		verbose: verbose,
	}
}

func (l *Logger) Println(v ...any) {
	if l == nil {
		log.Println(v...)
	}
	if l.verbose {
		l.logger.Println(v...)
	}
}

func (l *Logger) Printf(format string, v ...any) {
	if l == nil {
		log.Printf(format, v...)
	}
	if l.verbose {
		l.logger.Printf(format, v...)
	}
}

func (l *Logger) Fatal(v ...any) {
	if l == nil {
		log.Fatal(v...)
	}
	if l.verbose {
		l.logger.Fatal(v...)
	}
}
