package wal

import (
	"sync"
)

type rwLock struct {
	rwm *sync.RWMutex
}

func (l *rwLock) Lock() {
	l.rwm.RLock()
}

func (l *rwLock) Unlock() {
	l.rwm.RUnlock()
}
