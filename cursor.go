package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/mreiferson/wal/internal/util"
)

type Event struct {
	CRC  uint32
	ID   uint64
	Body []byte
}

type Cursor interface {
	ReadCh() <-chan Event
	Close() error
}

type cursor struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	offset     uint64
	startIdx   uint64
	idx        uint64
	segmentNum uint64

	wal     *wal
	version int32

	f      *os.File
	r      *bufio.Reader
	readCh chan Event

	closeCh chan struct{}
	wg      util.WaitGroupWrapper

	logger logger
}

func newCursor(w *wal, segmentNum uint64, idx uint64, offset uint64, logger logger) (Cursor, error) {
	c := &cursor{
		wal:        w,
		offset:     offset,
		startIdx:   idx,
		idx:        idx,
		segmentNum: segmentNum,
		readCh:     make(chan Event, 100), // TODO: (WAL) benchmark different buffer sizes
		closeCh:    make(chan struct{}),
		logger:     logger,
	}

	c.wg.Wrap(c.readLoop)

	return c, nil
}

func (c *cursor) logf(f string, args ...interface{}) {
	if c.logger == nil {
		return
	}
	c.logger.Output(2, fmt.Sprintf(f, args...))
}

func (c *cursor) ReadCh() <-chan Event {
	return c.readCh
}

func (c *cursor) Close() error {
	close(c.closeCh)
	c.wg.Wait()
	return nil
}

func (c *cursor) openFile() error {
	fn := segmentFileName(c.wal.dataPath, c.wal.name, c.segmentNum)
	c.logf("opening %s", fn)
	f, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	c.f = f
	c.r = bufio.NewReader(c.f)
	return c.readHeader()
}

func (c *cursor) isAtTail() bool {
	return c.segmentNum == c.wal.segmentNum && c.offset == c.wal.segment.offset
}

func (c *cursor) maybeWait() {
	c.wal.writeCond.L.Lock()
	for c.isAtTail() {
		c.wal.writeCond.Wait()
	}
	c.wal.writeCond.L.Unlock()
}

func (c *cursor) readLoop() {
	for {
		if c.f == nil {
			err := c.openFile()
			if err != nil {
				c.logf("ERROR: %s", err)
				c.roll()
				continue
			}
		}

		c.maybeWait()

		totalBytes, e, roll, err := readOne(c.r)
		if err != nil {
			c.logf("ERROR: readOne - %s", err)
			c.roll()
			continue
		}

		if roll {
			c.roll()
			continue
		}

		c.idx++
		c.offset += totalBytes

		if e.ID < c.startIdx {
			select {
			case <-c.closeCh:
				goto exit
			default:
			}
			continue
		}

		select {
		case c.readCh <- e:
		case <-c.closeCh:
			goto exit
		}
	}

exit:
}

func (c *cursor) roll() {
	// TODO: (WAL) if we roll past the end, signal write segment roll?
	c.f.Close()
	c.f = nil
	c.segmentNum++
	c.offset = 0

	c.wal.rollCond.L.Lock()
	for c.segmentNum > c.wal.segmentNum {
		c.wal.rollCond.Wait()
	}
	c.wal.rollCond.L.Unlock()
}

func (c *cursor) readHeader() error {
	var buf [4]byte

	_, err := c.f.ReadAt(buf[:], 0)
	if err != nil {
		return err
	}
	c.version = int32(binary.BigEndian.Uint32(buf[:]))

	if c.offset == 0 {
		c.offset = 4
	}

	_, err = c.f.Seek(int64(c.offset), 0)
	return err
}

func readOne(r io.Reader) (uint64, Event, bool, error) {
	var buf [4]byte

	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return 0, Event{}, false, err
	}
	size := int32(binary.BigEndian.Uint32(buf[:]))

	if size == eofIndicator {
		return 0, Event{}, true, nil
	}

	data := make([]byte, size)
	_, err = io.ReadFull(r, data)
	if err != nil {
		return 0, Event{}, false, err
	}

	return 4 + uint64(size), sliceToEvent(data), false, nil
}

func sliceToEvent(data []byte) Event {
	return Event{
		CRC:  binary.BigEndian.Uint32(data[:4]),
		ID:   binary.BigEndian.Uint64(data[4:12]),
		Body: data[12:],
	}
}
