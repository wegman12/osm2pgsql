package logger

import (
	"os"
	"sync"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	log  *zap.Logger
	once sync.Once
)

// Init initializes the global logger with console output only
func Init(debug bool) {
	once.Do(func() {
		initLogger(debug, "")
	})
}

// InitWithFile initializes the global logger with both console and file output
func InitWithFile(debug bool, logFile string) {
	once.Do(func() {
		initLogger(debug, logFile)
	})
}

// initLogger creates the logger with optional file output
func initLogger(debug bool, logFile string) {
	// Set level and encoder based on debug flag
	var level zapcore.Level
	var encoderConfig zapcore.EncoderConfig

	if debug {
		level = zapcore.DebugLevel
		encoderConfig = zap.NewDevelopmentEncoderConfig()
	} else {
		level = zapcore.InfoLevel
		encoderConfig = zap.NewProductionEncoderConfig()
	}

	// Console core (always present)
	consoleCore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(os.Stdout),
		level,
	)

	cores := []zapcore.Core{consoleCore}

	// Add file core if log file specified
	if logFile != "" {
		fileCore := zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			zapcore.AddSync(&lumberjack.Logger{
				Filename:   logFile,
				MaxSize:    50, // MB
				MaxBackups: 5,
				MaxAge:     30, // days
			}),
			level,
		)
		cores = append(cores, fileCore)
	}

	log = zap.New(zapcore.NewTee(cores...), zap.AddStacktrace(zapcore.ErrorLevel))
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
