// Package wal implements a write-ahead log
package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mreiferson/wal/internal/skiplist"
)

// EntryWriterTo is an interface representing an entry to be written
// to the log
type EntryWriterTo interface {
	WriteTo(io.Writer) (int64, error)
	CRC() uint32
	Len() int64
}

// WriteAheadLogger is the primary interface
type WriteAheadLogger interface {
	// Append adds entries to the log
	//
	// It returns the start and end indices and any error
	Append(entries []EntryWriterTo) (uint64, uint64, error)

	// AppendBytes adds entries to the log, it expects a crc to be provided for each entry
	//
	// It returns the start and end indices and any error
	AppendBytes(entries [][]byte, crc []uint32) (uint64, uint64, error)

	// Close flushes and cleanly closes the log
	Close() error

	// Delete permanently closes the log by deleting all data
	Delete() error

	// Empty destructively clears out any pending data in the log
	// by fast forwarding read positions and removing intermediate files
	Empty() error

	// Index returns the last index of the current segment
	Index() uint64

	// GetCursor returns a Cursor at the specified index
	GetCursor(idx uint64) (Cursor, error)
}

type wal struct {
	sync.RWMutex
	metadataLock sync.RWMutex

	name            string
	dataPath        string
	segmentMaxBytes int64
	syncTimeout     time.Duration
	version         int32

	idx           uint64
	segment       *segment
	segmentList   *skiplist.SkipList
	segmentNum    uint64
	segmentOffset uint64
	retentionNum  uint64

	writeCond *sync.Cond
	rollCond  *sync.Cond

	exitFlag int32
	exitChan chan int

	logger logger
}

type segmentListItem struct {
	segmentMap *segmentMap
	segmentNum uint64
}

// New returns a disk-backed WriteAheadLogger
func New(name string, dataPath string, segmentMaxBytes int64, syncTimeout time.Duration, logger logger) (WriteAheadLogger, error) {
	var err error

	w := &wal{
		name:            name,
		dataPath:        dataPath,
		segmentMaxBytes: segmentMaxBytes,
		exitChan:        make(chan int),
		syncTimeout:     syncTimeout,
		logger:          logger,
		version:         1,

		// uint64 keyed skip-list with *reverse* ordering so that
		// GetGreaterOrEqual actually returns LT | EQ
		segmentList: skiplist.NewCustomMap(func(l, r interface{}) bool {
			return l.(uint64) > r.(uint64)
		}),
	}

	rwl := &rwLock{&w.metadataLock}
	w.writeCond = sync.NewCond(rwl)
	w.rollCond = sync.NewCond(rwl)

	err = w.scanSegments()
	if err != nil {
		w.logf("ERROR: scanSegments - %s", err)
	}

	segment, err := w.openSegment(w.segmentNum, w.idx)
	if err != nil {
		return nil, err
	}
	w.segment = segment
	w.segmentOffset = w.segment.offset

	if w.syncTimeout > 0 {
		go w.syncLoop()
	}

	return w, err
}

func (w *wal) logf(f string, args ...interface{}) {
	if w.logger == nil {
		return
	}
	w.logger.Output(2, fmt.Sprintf(f, args...))
}

func (w *wal) scanSegments() error {
	mapRx := regexp.MustCompile(fmt.Sprintf(`%s\.wal\.([0-9]+)\.map`, w.name))
	maps, err := listDirRegexp(w.dataPath, mapRx)
	if err != nil {
		return err
	}

	if len(maps) == 0 {
		return nil
	}

	lastFn := maps[len(maps)-1]
	for _, fn := range maps {
		// this cannot fail based on the regex we're using
		segmentNum, _ := strconv.ParseInt(mapRx.FindStringSubmatch(fn)[1], 10, 64)

		// confirm we have a data file for this map
		dataFn := swapExtension(fn, "dat")
		_, err := os.Stat(path.Join(w.dataPath, dataFn))
		if os.IsNotExist(err) {
			w.logf("WARNING: found segment map without segment, removing (%s)", dataFn, fn)
			err := os.Remove(fn)
			if err != nil {
				w.logf("ERROR: failed to remove orphaned segment map - %s", err)
			}
			continue
		}

		segmentMap := newSegmentMap()
		err = segmentMap.load(path.Join(w.dataPath, fn))
		if err != nil {
			w.logf("WARNING: could not load segment map, rebuilding (%s)", fn)
			if !os.IsNotExist(err) {
				err := os.Remove(fn)
				if err != nil {
					w.logf("ERROR: failed to remove orphaned segment map - %s", err)
				}
			}
			segmentMap, err = rebuildSegmentMapFromSegment(path.Join(w.dataPath, fn))
			if err != nil {
				w.logf("ERROR: failed to rebuild segment map, removing segment (%s) - %s", dataFn, err)
				err := os.Remove(path.Join(w.dataPath, dataFn))
				if err != nil {
					w.logf("ERROR: failed to remove corrupt segment map - %s", err)
				}
				continue
			}
		}

		// segmentList only contains finished segments
		if fn != lastFn {
			w.segmentList.Set(segmentMap.startIdx(), segmentListItem{
				segmentMap: segmentMap,
				segmentNum: uint64(segmentNum),
			})
		}

		w.segmentNum = uint64(segmentNum)
	}

	// TODO: (WAL) iterate over segments / CRC?

	lastDataFn := swapExtension(lastFn, "dat")
	lastIdx, err := getLastSegmentIdx(path.Join(w.dataPath, lastDataFn))
	if err != nil {
		return err
	}
	w.idx = lastIdx + 1

	return nil
}

// Index returns the last index of the current segment
func (w *wal) Index() uint64 {
	w.metadataLock.RLock()
	defer w.metadataLock.RUnlock()
	return w.idx
}

func (w *wal) GetCursor(idx uint64) (Cursor, error) {
	segmentNum, err := w.segmentForIdx(idx)
	if err != nil {
		return nil, err
	}
	offset, err := w.nearestOffsetForIdx(segmentNum, idx)
	if err != nil {
		return nil, err
	}
	return newCursor(w, segmentNum, idx, offset,
		prefixedLogger(fmt.Sprintf("WAL(%s): ", w.name), w.logger))
}

func (w *wal) segmentForIdx(idx uint64) (uint64, error) {
	w.metadataLock.RLock()
	defer w.metadataLock.RUnlock()

	if idx > w.idx {
		return 0, errors.New("out of range")
	}

	// the current segment
	if w.segment.startIdx <= idx && idx <= w.segment.idx {
		return w.segmentNum, nil
	}

	// the rest of the segments
	_, value, present := w.segmentList.GetGreaterOrEqual(idx)
	if !present {
		return 0, errors.New("out of range")
	}
	return value.(segmentListItem).segmentNum, nil
}

func (w *wal) nearestOffsetForIdx(segmentNum uint64, idx uint64) (uint64, error) {
	sm := newSegmentMap()
	err := sm.load(segmentMapFileName(w.dataPath, w.name, segmentNum))
	if err != nil {
		return 0, err
	}
	return sm.locate(idx)
}

func (w *wal) openSegment(segmentNum uint64, idx uint64) (*segment, error) {
	fn := segmentFileName(w.dataPath, w.name, segmentNum)
	return newSegment(fn, idx,
		prefixedLogger(fmt.Sprintf("WAL(%s): ", w.name), w.logger))
}

// Append adds entries to the log
//
// It returns the start and end indices and any error
func (w *wal) Append(entries []EntryWriterTo) (uint64, uint64, error) {
	var err error

	sizeWithHeader := int64(len(entries)) * 16
	for _, e := range entries {
		sizeWithHeader += e.Len()
	}

	if sizeWithHeader >= w.segmentMaxBytes {
		return 0, 0, fmt.Errorf("chunk too large %d > %d", sizeWithHeader, w.segmentMaxBytes)
	}

	w.Lock()
	defer w.Unlock()

	if w.exitFlag == 1 {
		return 0, 0, errors.New("exiting")
	}

	segment, segmentNum, err := w.maybeRollSegment(sizeWithHeader)
	if err != nil {
		return 0, 0, err
	}

	idx, err := segment.append(entries)
	if err != nil {
		return 0, 0, err
	}

	startIdx := w.idx
	endIdx := idx - 1

	w.broadcast(segment, segmentNum, idx)

	return startIdx, endIdx, err
}

// AppendBytes adds entries to the log, it expects a crc to be provided for each entry
//
// It returns the start and end indices and any error
func (w *wal) AppendBytes(entries [][]byte, crc []uint32) (uint64, uint64, error) {
	var err error

	sizeWithHeader := int64(len(entries)) * 16
	for _, e := range entries {
		sizeWithHeader += int64(len(e))
	}

	if sizeWithHeader >= w.segmentMaxBytes {
		return 0, 0, fmt.Errorf("chunk too large %d > %d", sizeWithHeader, w.segmentMaxBytes)
	}

	if len(entries) != len(crc) {
		return 0, 0, fmt.Errorf("must provide crc for each entry %d != %d", len(entries), len(crc))
	}

	w.Lock()
	defer w.Unlock()

	if w.exitFlag == 1 {
		return 0, 0, errors.New("exiting")
	}

	segment, segmentNum, err := w.maybeRollSegment(sizeWithHeader)
	if err != nil {
		return 0, 0, err
	}

	idx, err := segment.appendBytes(entries, crc)
	if err != nil {
		return 0, 0, err
	}

	startIdx := w.idx
	endIdx := idx - 1

	w.broadcast(segment, segmentNum, idx)

	return startIdx, endIdx, err
}

func (w *wal) maybeRollSegment(sizeWithHeader int64) (*segment, uint64, error) {
	var err error

	segment := w.segment
	segmentNum := w.segmentNum

	if segment != nil && segment.size()+sizeWithHeader >= w.segmentMaxBytes {
		segmentNum++

		err = w.sync()
		if err != nil {
			w.logf("ERROR: wal(%s) failed to sync - %s", w.name, err)
		}

		segment.finish()
		segment = nil
	}

	if segment == nil {
		segment, err = w.openSegment(segmentNum, w.idx)
		if err != nil {
			return nil, 0, err
		}
	}

	return segment, segmentNum, nil
}

func (w *wal) broadcast(segment *segment, segmentNum uint64, idx uint64) {
	w.metadataLock.Lock()
	if w.segmentNum != segmentNum {
		w.segmentList.Set(w.segment.segmentMap.startIdx(), segmentListItem{
			segmentMap: w.segment.segmentMap,
			segmentNum: w.segmentNum,
		})
		w.segment = segment
		w.rollCond.Broadcast()
	} else {
		w.segment = segment
	}
	w.segmentOffset = w.segment.offset
	w.segmentNum = segmentNum
	w.idx = idx
	w.writeCond.Broadcast()
	w.metadataLock.Unlock()
}

func (w *wal) Close() error {
	err := w.exit(false)
	if err != nil {
		return err
	}
	return w.sync()
}

func (w *wal) Delete() error {
	w.Empty()
	return w.exit(true)
}

func (w *wal) exit(deleted bool) error {
	w.Lock()
	defer w.Unlock()

	w.exitFlag = 1

	if deleted {
		w.logf("WAL(%s): deleting", w.name)
	} else {
		w.logf("WAL(%s): closing", w.name)
	}

	close(w.exitChan)

	if w.segment != nil {
		w.segment.close()
		w.segment = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the log
// by fast forwarding read positions and removing intermediate files
func (w *wal) Empty() error {
	w.Lock()
	defer w.Unlock()

	if w.exitFlag == 1 {
		return errors.New("exiting")
	}

	w.logf("WAL(%s): emptying", w.name)

	return w.deleteAll()
}

func (w *wal) deleteAll() error {
	var err error

	if w.segment != nil {
		w.segment.close()
		w.segment = nil
	}

	for i := w.retentionNum; i <= w.segmentNum; i++ {
		for _, fn := range []string{
			segmentFileName(w.dataPath, w.name, i),
			segmentMapFileName(w.dataPath, w.name, i)} {
			w.logf("WAL(%s): removing %s", w.name, fn)
			innerErr := os.Remove(fn)
			if innerErr != nil && !os.IsNotExist(innerErr) {
				w.logf("ERROR: wal(%s) failed to remove data file - %s", w.name, innerErr)
				err = innerErr
			}
		}
	}

	w.metadataLock.Lock()
	w.segmentNum++
	w.retentionNum = w.segmentNum
	w.metadataLock.Unlock()

	return err
}

// sync fsyncs the current segment
func (w *wal) sync() error {
	if w.segment == nil {
		return nil
	}

	err := w.segment.sync()
	if err != nil {
		w.segment.close()
		w.segment = nil
	}
	return err
}

func (w *wal) syncLoop() {
	syncTicker := time.NewTicker(w.syncTimeout)

	for {
		select {
		case <-syncTicker.C:
			err := w.sync()
			if err != nil {
				w.logf("ERROR: wal(%s) failed to sync - %s", w.name, err)
			}
		case <-w.exitChan:
			goto exit
		}
	}

exit:
	w.logf("WAL(%s): closing ... syncLoop", w.name)
	syncTicker.Stop()
}

func listDirRegexp(dir string, rx *regexp.Regexp) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(_ string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if fi.IsDir() || !rx.MatchString(fi.Name()) {
			return nil
		}

		files = append(files, fi.Name())
		return nil
	})
	return files, err
}

func segmentMapFileName(dataPath string, name string, fileNum uint64) string {
	return fmt.Sprintf(path.Join(dataPath, "%s.wal.%09d.map"), name, fileNum)
}

func segmentFileName(dataPath string, name string, fileNum uint64) string {
	return fmt.Sprintf(path.Join(dataPath, "%s.wal.%09d.dat"), name, fileNum)
}

func swapExtension(fn string, ext string) string {
	return strings.Replace(fn, path.Ext(fn), "."+ext, -1)
}
