package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

const eofIndicator int32 = -1
const segmentMapFlushInterval = 200

type segment struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	offset   uint64
	startIdx uint64
	idx      uint64
	count    uint64

	fileName string
	version  int32

	segmentMap *segmentMap

	f   *os.File
	buf bytes.Buffer

	logger logger
}

func newSegment(fileName string, idx uint64, logger logger) (*segment, error) {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	s := &segment{
		startIdx:   idx,
		idx:        idx,
		f:          f,
		fileName:   fileName,
		logger:     logger,
		segmentMap: newSegmentMap(),
		offset:     uint64(fi.Size()),
	}

	segmentMapFn := swapExtension(fileName, "map")
	if fi.Size() == 0 {
		err := s.writeHeader()
		if err != nil {
			s.close()
			return nil, err
		}
		s.segmentMap.set(s.idx, s.offset)
		err = s.segmentMap.flush(segmentMapFn)
		if err != nil {
			return nil, err
		}
		s.logf("created %s at %d", s.fileName, s.idx)
	} else {
		err := s.segmentMap.load(segmentMapFn)
		if err != nil {
			return nil, err
		}
		s.logf("re-opened %s at %d", s.fileName, s.idx)
	}

	return s, nil
}

func (s *segment) logf(f string, args ...interface{}) {
	if s.logger == nil {
		return
	}
	s.logger.Output(2, fmt.Sprintf(f, args...))
}

func (s *segment) finish() error {
	err := s.writeTrailer()
	if err != nil {
		s.logf("ERROR: writeTrailer - %s", err)
	}
	return s.close()
}

func (s *segment) close() error {
	return s.f.Close()
}

func (s *segment) writeHeader() error {
	err := binary.Write(s.f, binary.BigEndian, int32(s.version))
	if err != nil {
		return err
	}
	s.offset += 4
	return nil
}

func (s *segment) writeTrailer() error {
	err := binary.Write(s.f, binary.BigEndian, eofIndicator)
	if err != nil {
		return err
	}
	s.offset += 4
	return nil
}

func (s *segment) append(data [][]byte, crcs []uint32) (uint64, error) {
	var err error
	var totalBytes int
	var buf [16]byte
	var segmentMapItems [][2]uint64

	s.buf.Reset()

	idx := s.idx
	count := s.count
	offset := s.offset
	for i, entry := range data {
		size := 4 /* CRC */ + 8 /* ID */ + uint32(len(entry))

		binary.BigEndian.PutUint32(buf[:4], size)
		binary.BigEndian.PutUint32(buf[4:8], crcs[i])
		binary.BigEndian.PutUint64(buf[8:16], uint64(idx))

		_, err = s.buf.Write(buf[:])
		if err != nil {
			goto exit
		}

		_, err = s.buf.Write(entry)
		if err != nil {
			goto exit
		}

		if count%segmentMapFlushInterval == 0 {
			segmentMapItems = append(segmentMapItems, [2]uint64{idx, offset})
		}

		idx++
		count++
		offset += 4 + uint64(size)
	}

	// TODO: (WAL) can we use writev here to avoid copying?
	// only write to the file once
	totalBytes, err = s.f.Write(s.buf.Bytes())
	if err != nil {
		goto exit
	}

	s.idx = idx
	s.offset += uint64(totalBytes)
	s.count += count

	if len(segmentMapItems) > 0 {
		for _, item := range segmentMapItems {
			s.segmentMap.set(item[0], item[1])
		}
		err = s.segmentMap.flush(swapExtension(s.fileName, "map"))
		if err != nil {
			goto exit
		}
	}

exit:
	return s.idx, err
}

func (s *segment) sync() error {
	if s.f == nil {
		return nil
	}
	return s.f.Sync()
}

func (s *segment) size() int64 {
	return int64(s.offset)
}

func getLastSegmentIdx(fn string) (uint64, error) {
	mapFn := swapExtension(fn, "map")
	segmentMap := newSegmentMap()
	err := segmentMap.load(mapFn)
	if err != nil {
		return 0, err
	}

	f, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := uint64(fi.Size())

	offset := segmentMap.lastOffset()
	_, err = f.Seek(int64(offset), 0)
	if err != nil {
		return 0, err
	}
	r := bufio.NewReader(f)

	var idx uint64
	for offset < size {
		totalBytes, e, _, err := readOne(r)
		if err != nil {
			return 0, err
		}
		offset += totalBytes
		idx = e.ID
	}

	return idx, nil
}
