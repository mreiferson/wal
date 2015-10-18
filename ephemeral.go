package wal

type ephemeralWAL struct {
	ch  chan Entry
	idx uint64
}

// NewEphemeral returns a memory-backed WriteAheadLogger
func NewEphemeral() WriteAheadLogger {
	return &ephemeralWAL{
		ch: make(chan Entry, 1000),
	}
}

func (e *ephemeralWAL) Append(entries [][]byte, crc []uint32) (uint64, uint64, error) {
	idxStart := e.idx
	for _, entry := range entries {
		e.ch <- Entry{
			Body: entry,
			ID:   e.idx,
		}
		e.idx++
	}
	return idxStart, e.idx - 1, nil
}

func (e *ephemeralWAL) GetCursor(idx uint64) (Cursor, error) {
	return ephemeralCursor{
		readCh: e.ch,
	}, nil
}

func (e *ephemeralWAL) Close() error {
	close(e.ch)
	return nil
}

func (e *ephemeralWAL) Delete() error {
	return nil
}

func (e *ephemeralWAL) Empty() error {
	return nil
}

type ephemeralCursor struct {
	readCh chan Entry
}

func (c ephemeralCursor) ReadCh() <-chan Entry {
	return c.readCh
}

func (c ephemeralCursor) Close() error {
	return nil
}
