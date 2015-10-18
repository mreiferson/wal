package wal

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"os"

	"github.com/mreiferson/wal/internal/atomic_rename"
	"github.com/mreiferson/wal/internal/skiplist"
)

type segmentMap struct {
	entries *skiplist.SkipList
}

func newSegmentMap() *segmentMap {
	return &segmentMap{
		entries: skiplist.NewCustomMap(func(l, r interface{}) bool {
			return l.(uint64) > r.(uint64)
		}),
	}
}

func (sm *segmentMap) load(fn string) error {
	f, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var idx uint64
		var offset uint64
		_, err = fmt.Sscanf(scanner.Text(), "%d %d", &idx, &offset)
		if err != nil {
			return err
		}
		sm.entries.Set(idx, offset)
	}
	return scanner.Err()
}

func (sm *segmentMap) locate(idx uint64) (uint64, error) {
	_, value, present := sm.entries.GetGreaterOrEqual(idx)
	if !present {
		return 0, errors.New("not found")
	}
	return value.(uint64), nil
}

func (sm *segmentMap) set(idx uint64, offset uint64) {
	sm.entries.Set(idx, offset)
}

func (sm *segmentMap) flush(fn string) error {
	tmpFn := fmt.Sprintf("%s.%d.tmp", fn, rand.Int())
	f, err := os.OpenFile(tmpFn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	i := sm.entries.Iterator()
	for i.Next() {
		_, err := fmt.Fprintf(f, "%d %d\n", i.Key(), i.Value())
		if err != nil {
			f.Close()
			return err
		}
	}
	f.Sync()
	f.Close()

	return atomic_rename.Rename(tmpFn, fn)
}

func (sm *segmentMap) startIdx() uint64 {
	return sm.entries.SeekToLast().Key().(uint64)
}

func (sm *segmentMap) lastOffset() uint64 {
	return sm.entries.SeekToFirst().Value().(uint64)
}

func rebuildSegmentMapFromSegment(fn string) (*segmentMap, error) {
	segmentMap := newSegmentMap()

	f, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_, err = f.Seek(4, 0)
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(f)

	var count uint64
	var offset uint64 = 4
	for {
		totalBytes, e, roll, err := readOne(r)
		if err != nil {
			return nil, err
		}
		// TODO: (WAL) this isn't going to work for the current segment
		if roll {
			break
		}
		if count%segmentMapFlushInterval == 0 {
			segmentMap.set(e.ID, offset)
		}
		offset += totalBytes
		count++
	}

	err = segmentMap.flush(swapExtension(fn, "map"))
	if err != nil {
		return nil, err
	}

	return segmentMap, nil
}
