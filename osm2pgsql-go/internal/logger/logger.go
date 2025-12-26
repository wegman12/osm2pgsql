package logger

import (
	"os"
	"sync"

	"go.uber.org/zap"
)

var (
	log  *zap.SugaredLogger
	once sync.Once
)

// Init initializes the global logger
func Init(debug bool) {
	once.Do(func() {
		var logger *zap.Logger
		var err error

		if debug {
			logger, err = zap.NewDevelopment()
		} else {
			// Use development config but at info level for readable output
			cfg := zap.NewDevelopmentConfig()
			cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
			cfg.DisableCaller = true
			cfg.DisableStacktrace = true
			logger, err = cfg.Build()
		}

		if err != nil {
			logger = zap.NewExample()
		}

		log = logger.Sugar()
	})
}

// Get returns the global logger
func Get() *zap.SugaredLogger {
	if log == nil {
		Init(false)
	}
	return log
}

// L is a shorthand for Get()
func L() *zap.SugaredLogger {
	return Get()
}

// Sync flushes any buffered log entries
func Sync() {
	if log != nil {
		log.Sync()
	}
}

// Info logs an info message
func Info(msg string, keysAndValues ...interface{}) {
	Get().Infow(msg, keysAndValues...)
}

// Debug logs a debug message
func Debug(msg string, keysAndValues ...interface{}) {
	Get().Debugw(msg, keysAndValues...)
}

// Warn logs a warning message
func Warn(msg string, keysAndValues ...interface{}) {
	Get().Warnw(msg, keysAndValues...)
}

// Error logs an error message
func Error(msg string, keysAndValues ...interface{}) {
	Get().Errorw(msg, keysAndValues...)
}

// Fatal logs a fatal message and exits
func Fatal(msg string, keysAndValues ...interface{}) {
	Get().Fatalw(msg, keysAndValues...)
	os.Exit(1)
}

// With creates a child logger with additional context
func With(keysAndValues ...interface{}) *zap.SugaredLogger {
	return Get().With(keysAndValues...)
}

// Progress logs progress updates (info level with consistent format)
func Progress(msg string, current, total int64) {
	Get().Infow(msg, "current", current, "total", total, "pct", float64(current)/float64(total)*100)
}
