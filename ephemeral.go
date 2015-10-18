package wal

type ephemeralWAL struct {
	ch  chan Event
	idx uint64
}

func NewEphemeral() WriteAheadLogger {
	return &ephemeralWAL{
		ch: make(chan Event, 1000),
	}
}

func (e *ephemeralWAL) Append(data [][]byte, crc []uint32) (uint64, uint64, error) {
	idxStart := e.idx
	for _, entry := range data {
		e.ch <- Event{
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
	readCh chan Event
}

func (c ephemeralCursor) ReadCh() <-chan Event {
	return c.readCh
}

func (c ephemeralCursor) Close() error {
	return nil
}
