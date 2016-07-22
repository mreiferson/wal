package wal

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/crc32"
)

func equal(t *testing.T, act, exp interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

func nequal(t *testing.T, act, exp interface{}) {
	if reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

type tbLog interface {
	Log(...interface{})
}

type testLogger struct {
	tbLog
}

func (tl *testLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func newTestLogger(tbl tbLog) logger {
	return &testLogger{tbl}
}

func crcAppend(w WriteAheadLogger, entries [][]byte) (uint64, uint64, error) {
	crc := make([]uint32, 0, len(entries))
	for _, d := range entries {
		crc = append(crc, crc32.ChecksumIEEE(d))
	}
	return w.AppendBytes(entries, crc)
}

func TestWAL(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("wal-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	wsize := int64(1024 * 1024)
	msize := 4096
	bsize := 250
	bnum := 3

	w, err := New("test", tmpDir, wsize, 0, newTestLogger(t))
	equal(t, err, nil)
	nequal(t, w, nil)
	defer os.RemoveAll(tmpDir)
	defer w.Delete()

	var entries [][]byte
	for i := 0; i < bsize; i++ {
		entries = append(entries, make([]byte, msize))
	}

	for i := 0; i < bnum; i++ {
		_, _, err := crcAppend(w, entries)
		equal(t, err, nil)
	}

	equal(t, w.(*wal).segmentList.Len(), 2)
	i := w.(*wal).segmentList.Iterator()
	i.Next()
	equal(t, i.Key().(uint64), uint64(250))
	equal(t, i.Value().(segmentListItem).segmentNum, uint64(1))
	equal(t, i.Value().(segmentListItem).segmentMap.startIdx(), uint64(250))
	i.Next()
	equal(t, i.Key().(uint64), uint64(0))
	equal(t, i.Value().(segmentListItem).segmentNum, uint64(0))
	equal(t, i.Value().(segmentListItem).segmentMap.startIdx(), uint64(0))

	c, err := w.GetCursor(0)
	equal(t, err, nil)

	for i := 0; i < bsize*bnum; i++ {
		e := <-c.ReadCh()
		t.Logf("id: %d", e.ID)
		equal(t, e.Body, entries[i%bsize])
	}
}

func TestWALCursor(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("wal-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	wsize := int64(1024 * 1024)
	msize := 4096
	bsize := 250
	bnum := 3

	w, err := New("test", tmpDir, wsize, 0, newTestLogger(t))
	equal(t, err, nil)
	nequal(t, w, nil)
	defer os.RemoveAll(tmpDir)
	defer w.Delete()

	var entries [][]byte
	for i := 0; i < bsize; i++ {
		entries = append(entries, make([]byte, msize))
	}

	for i := 0; i < bnum; i++ {
		_, _, err := crcAppend(w, entries)
		equal(t, err, nil)
	}

	c, err := w.GetCursor(100)
	equal(t, err, nil)

	for i := 0; i < bsize*bnum-100; i++ {
		e := <-c.ReadCh()
		t.Logf("id: %d", e.ID)
		equal(t, e.Body, entries[i%bsize])
	}
}

func TestWALReopen(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("wal-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	wsize := int64(1024 * 1024)
	msize := 4096
	bsize := 250
	bnum := 3

	w, err := New("test", tmpDir, wsize, 0, newTestLogger(t))
	equal(t, err, nil)
	nequal(t, w, nil)
	defer os.RemoveAll(tmpDir)

	var entries [][]byte
	for i := 0; i < bsize; i++ {
		entries = append(entries, make([]byte, msize))
	}

	for i := 0; i < bnum; i++ {
		_, _, err := crcAppend(w, entries)
		equal(t, err, nil)
	}

	err = w.Close()
	equal(t, err, nil)

	w, err = New("test", tmpDir, wsize, 0, newTestLogger(t))
	equal(t, err, nil)
	nequal(t, w, nil)
	defer w.Delete()

	for i := 0; i < bnum; i++ {
		_, _, err := crcAppend(w, entries)
		equal(t, err, nil)
	}
}

func TestWALParallelReadWrite(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("wal-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	w, err := New("test", tmpDir, 1024*1024, 0, log.New(os.Stderr, "", log.LstdFlags))
	equal(t, err, nil)
	nequal(t, w, nil)
	defer os.RemoveAll(tmpDir)
	defer w.Delete()

	var entries [][]byte
	for i := 0; i < 25; i++ {
		entries = append(entries, make([]byte, 4096))
	}

	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	waitCh := make(chan struct{})
	startCh := make(chan struct{})

	wg.Add(1)
	go func() {
		waitCh <- struct{}{}
		<-startCh
		for i := 0; i < 1000; i++ {
			_, _, err := crcAppend(w, entries)
			if err != nil {
				t.Fatal(err.Error())
			}
			time.Sleep(time.Microsecond * 100)
		}
		wg.Done()
	}()
	<-waitCh

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			c, err := w.GetCursor(0)
			if err != nil {
				t.Fatal(err.Error())
			}

			waitCh <- struct{}{}
			<-startCh
			for i := 0; i < 25000; i++ {
				e := <-c.ReadCh()
				if i%1000 == 0 || i == 24999 {
					t.Logf("(%d) id: %d", i, e.ID)
				}
			}
			wg.Done()
		}()
		<-waitCh
	}

	close(startCh)
	wg.Wait()
}

func BenchmarkWALAppend256(b *testing.B)  { benchmarkWALAppend(b, 256) }
func BenchmarkWALAppend512(b *testing.B)  { benchmarkWALAppend(b, 512) }
func BenchmarkWALAppend1k(b *testing.B)   { benchmarkWALAppend(b, 1024) }
func BenchmarkWALAppend2k(b *testing.B)   { benchmarkWALAppend(b, 2*1024) }
func BenchmarkWALAppend4k(b *testing.B)   { benchmarkWALAppend(b, 4*1024) }
func BenchmarkWALAppend8k(b *testing.B)   { benchmarkWALAppend(b, 8*1024) }
func BenchmarkWALAppend16k(b *testing.B)  { benchmarkWALAppend(b, 16*1024) }
func BenchmarkWALAppend32k(b *testing.B)  { benchmarkWALAppend(b, 32*1024) }
func BenchmarkWALAppend64k(b *testing.B)  { benchmarkWALAppend(b, 64*1024) }
func BenchmarkWALAppend128k(b *testing.B) { benchmarkWALAppend(b, 128*1024) }
func BenchmarkWALAppend256k(b *testing.B) { benchmarkWALAppend(b, 256*1024) }
func BenchmarkWALAppend512k(b *testing.B) { benchmarkWALAppend(b, 512*1024) }
func BenchmarkWALAppend1m(b *testing.B)   { benchmarkWALAppend(b, 1024*1024) }

func benchmarkWALAppend(b *testing.B, size int) {
	b.StopTimer()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("wal-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	w, _ := New("bench", tmpDir, 1024*1024*100, 0, newTestLogger(b))
	defer os.RemoveAll(tmpDir)
	defer w.Delete()

	var entries [][]byte
	for i := 0; i < 1024*1024/size; i++ {
		entries = append(entries, make([]byte, size))
	}

	b.SetBytes(int64(size * len(entries) * runtime.GOMAXPROCS(0)))

	var wg sync.WaitGroup
	waitCh := make(chan struct{})
	startCh := make(chan struct{})

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go func() {
			waitCh <- struct{}{}
			<-startCh
			for i := 0; i < b.N; i++ {
				_, _, err := crcAppend(w, entries)
				if err != nil {
					b.Fatal(err.Error())
				}
			}
			wg.Done()
		}()
		<-waitCh
	}

	b.StartTimer()

	close(startCh)
	wg.Wait()

	b.StopTimer()
}

func BenchmarkWALRead(b *testing.B) {
	b.StopTimer()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("wal-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	w, _ := New("bench", tmpDir, 1024*1024*100, 0, newTestLogger(b))
	defer os.RemoveAll(tmpDir)
	defer w.Delete()

	sz := 200
	num := 1000

	var entries [][]byte
	for i := 0; i < num; i++ {
		entries = append(entries, make([]byte, sz))
	}

	b.SetBytes(int64(sz * len(entries) * runtime.GOMAXPROCS(0)))

	for i := 0; i < b.N; i++ {
		_, _, err := crcAppend(w, entries)
		if err != nil {
			b.Fatal(err.Error())
		}
	}

	var wg sync.WaitGroup
	waitCh := make(chan struct{})
	startCh := make(chan struct{})

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go func() {
			waitCh <- struct{}{}
			<-startCh
			c, err := w.GetCursor(0)
			if err != nil {
				b.Fatal(err.Error())
			}
			ch := c.ReadCh()
			for i := 0; i < b.N*len(entries); i++ {
				<-ch
			}
			wg.Done()
		}()
		<-waitCh
	}

	b.StartTimer()

	close(startCh)
	wg.Wait()

	b.StopTimer()
}
