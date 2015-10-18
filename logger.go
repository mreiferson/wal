package wal

import "fmt"

type logger interface {
	Output(maxdepth int, s string) error
}

type prefixLogger struct {
	l logger
	p string
}

func (l prefixLogger) Output(d int, s string) error {
	return l.l.Output(d, fmt.Sprintf("%s%s", l.p, s))
}

func prefixedLogger(p string, l logger) logger {
	return prefixLogger{l: l, p: p}
}
