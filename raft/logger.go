package raft

import (
	"fmt"
	"log"
	"strings"
)

type Loglevel uint8

const (
	Error Loglevel = iota
	Warning
	Info
	Debug
)

func (level Loglevel) String() string {
	switch level {
	case Error:
		return "Error"
	case Warning:
		return "Warning"
	case Info:
		return "Info"
	case Debug:
		return "Debug"
	}

	return "Unknown"
}

type Logger struct {
	level Loglevel
	w     *log.Logger
}

func NewLogger(level Loglevel) *Logger {
	return &Logger{
		level: level,
		w:     &log.Logger{},
	}
}

func (l *Logger) Log(level Loglevel, msg string, args ...any) {
	msg = strings.TrimRight(msg, "\n")
	out := fmt.Sprintf("%s\n", args...)
	if l.level >= level {
		l.w.Printf("%s: %s", level.String(), out)
	}
}

func (l *Logger) Error(msg string, args ...any) {
	l.Log(Error, msg, args...)
}

func (l *Logger) Warning(msg string, args ...any) {
	l.Log(Warning, msg, args...)
}

func (l *Logger) Info(msg string, args ...any) {
	l.Log(Info, msg, args...)
}

func (l *Logger) Debug(msg string, args ...any) {
	l.Log(Debug, msg, args...)
}
