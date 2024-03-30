package logger

import (
	"github.com/sanjiv-madhavan/dynamodb-delete-partition/internal/ddb/middleware"
	"go.uber.org/zap"
)

type Logger struct {
	Log        *zap.Logger
	Middleware *middleware.Middleware
}

func NewLogger(log *zap.Logger, middleware *middleware.Middleware) *Logger {
	return &Logger{
		Log:        log,
		Middleware: middleware,
	}
}
