package logger

import (
	"sync"

	"go.uber.org/zap"
)

var (
	log  *zap.Logger
	once sync.Once
)

// Init initializes the global logger
func Init(debug bool) {
	once.Do(func() {
		var err error

		if debug {
			log, err = zap.NewDevelopment()
		} else {
			log, err = zap.NewDevelopment()
		}

		if err != nil {
			log = zap.NewExample()
		}
	})
}

// Get returns the global logger
func Get() *zap.Logger {
	if log == nil {
		Init(false)
	}
	return log
}

// Sync flushes any buffered log entries
func Sync() {
	if log != nil {
		log.Sync()
	}
}
