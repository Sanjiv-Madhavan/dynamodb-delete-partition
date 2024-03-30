package middleware

import (
	"context"

	"go.uber.org/zap"
)

type Middleware struct {
	log *zap.Logger
}

func NewMiddleware() *Middleware {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{"stdout"}

	logger, err := cfg.Build()
	if err != nil {
		panic("failed to instantiate middleware")
	}

	// Example usage
	logger.Info("Logging to stdout with middleware")

	return &Middleware{
		log: logger,
	}
}

// LogHandler prints the provided information using the logger
func (m *Middleware) LogHandler(ctx context.Context, msg string, args ...interface{}) {
	m.log.Info(msg, convertToZapFields(args)...)
}

func (m *Middleware) LogInfo(message string) {
	m.log.Error(message)
}

// LogError prints the provided error using the logger
func (m *Middleware) LogError(message string, err error) {
	m.log.Error(message, zap.Error(err))
}

func (m *Middleware) LogFatal(message string, err error) {
	m.log.Fatal(message, zap.Error(err))
}

func convertToZapFields(args ...interface{}) []zap.Field {
	fields := make([]zap.Field, 0)
	if len(args)%2 != 0 {
		// If the number of arguments is odd, ignore the last one
		args = args[:len(args)-1]
	}

	for i := 0; i < len(args); i += 2 {
		if key, ok := args[i].(string); ok {
			fields = append(fields, zap.Any(key, args[i+1]))
		}
	}

	return fields
}
