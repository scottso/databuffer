package databuffer

import (
	"fmt"
	"log/slog"
	"os"
)

func newLogger[T any]() *slog.Logger {
	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})).
		With(slog.String("databuffer_type", fmt.Sprintf("%T", *new(T))))

	return l
}
