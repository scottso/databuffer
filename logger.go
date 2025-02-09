package databuffer

import (
	"fmt"
	"log/slog"
	"os"
)

func newLogger[T any]() *slog.Logger {
	l := slog.New(slog.NewJSONHandler(os.Stdout, nil)).
		With(slog.String("databuffer_type", fmt.Sprintf("%T", *new(T))))

	slog.SetDefault(l)
	slog.SetLogLoggerLevel(slog.LevelDebug)

	return l
}
