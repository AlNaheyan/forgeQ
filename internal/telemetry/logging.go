// Package telemetry contains ForgeQueue's shared observability helpers.
package telemetry

import (
	"fmt"
	"io"
	"log/slog"
)

// NewLogger creates a JSON logger with stable field names and a service field
// attached to every record.
func NewLogger(output io.Writer, service, level string) (*slog.Logger, error) {
	var slogLevel slog.Level
	switch level {
	case "debug":
		slogLevel = slog.LevelDebug
	case "info":
		slogLevel = slog.LevelInfo
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level %q", level)
	}

	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level: slogLevel,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			switch attr.Key {
			case slog.TimeKey:
				attr.Key = "timestamp"
			case slog.MessageKey:
				attr.Key = "message"
			}
			return attr
		},
	})

	return slog.New(handler).With("service", service), nil
}
