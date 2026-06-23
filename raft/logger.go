package raft

import "log"

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

func (l *Logger) Log(level Loglevel, msg string) {
	if l.level >= Error {
		l.w.Printf("%s: %s\n", level.String(), msg)
	}
}

func (l *Logger) Error(msg string) {
	l.Log(Error, msg)
}

func (l *Logger) Warning(msg string) {
	l.Log(Warning, msg)
}

func (l *Logger) Info(msg string) {
	l.Log(Info, msg)
}

func (l *Logger) Debug(msg string) {
	l.Log(Debug, msg)
}
