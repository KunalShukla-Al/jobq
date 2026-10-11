package logstore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// segment is one file of the log, named by the log offset of its first byte
// (00000000000000000000.log, …), so a listing sorts in log order.
type segment struct {
	base int64
	path string
	size int64
}

func segmentName(base int64) string { return fmt.Sprintf("%020d.log", base) }

// files is the segment files of one log directory. Only the newest segment
// is ever written to.
type files struct {
	dir      string
	segs     []segment
	active   *os.File
	fullSync bool
}

// listSegments finds the segment files in dir, oldest first.
func listSegments(dir string) ([]segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []segment
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") || len(name) != len(segmentName(0)) {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		segs = append(segs, segment{base: base, path: filepath.Join(dir, name), size: info.Size()})
	}
	slices.SortFunc(segs, func(a, b segment) int { return int(a.base - b.base) })
	return segs, nil
}

// Recovery is what opening the log found and did.
type Recovery struct {
	Segments  int           // segment files read
	Records   int           // records replayed
	Jobs      int           // jobs in the rebuilt state
	Truncated int64         // bytes cut from the end of the newest segment (a torn write)
	Obsolete  int           // segments before the newest complete checkpoint, deleted
	Took      time.Duration // the whole replay
}

// recover reads every segment in order, applies each record to a fresh
// state, and repairs the tail:
//
//   - At the first torn or corrupt record of the newest segment, the file is
//     cut there. Everything before it was acknowledged; nothing after it
//     was, because an answer waits for the fsync that covers its records.
//   - A torn record in an older segment can't come from a crash (only the
//     newest is written), so it's an error: the log is damaged and recovery
//     refuses to guess.
//   - A segment that starts with a checkpoint replaces everything before it,
//     but only once its checkpoint-end is read; a checkpoint cut short by a
//     crash is dropped and the older segments still hold the state.
func recover(dir string) (*state, []segment, Recovery, error) {
	began := time.Now()
	var rec Recovery
	segs, err := listSegments(dir)
	if err != nil {
		return nil, nil, rec, err
	}
	st := newState()
	keepFrom := 0 // index of the first segment still needed
	for i := range segs {
		newest := i == len(segs)-1
		next, good, n, err := replaySegment(segs[i], st)
		rec.Segments++
		rec.Records += n
		if err != nil && !(errors.Is(err, errTorn) && newest) {
			return nil, nil, rec, fmt.Errorf("%s: %w", filepath.Base(segs[i].path), err)
		}
		if next != st {
			keepFrom = i // a complete checkpoint: older segments are obsolete
		}
		st = next
		if good < segs[i].size {
			if err := os.Truncate(segs[i].path, good); err != nil {
				return nil, nil, rec, err
			}
			rec.Truncated = segs[i].size - good
			segs[i].size = good
		}
	}
	for _, s := range segs[:keepFrom] {
		if err := os.Remove(s.path); err != nil {
			return nil, nil, rec, err
		}
		rec.Obsolete++
	}
	segs = segs[keepFrom:]
	rec.Jobs = len(st.jobs)
	rec.Took = time.Since(began)
	return st, segs, rec, nil
}

// replaySegment applies one segment's records to st and returns the state
// to carry on with (a new one if the segment holds a complete checkpoint),
// how many bytes from the start are good, and how many records it applied.
func replaySegment(seg segment, st *state) (*state, int64, int, error) {
	f, err := os.Open(seg.path)
	if err != nil {
		return st, 0, 0, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)

	var offset, good int64 // good: the end of the last record outside an unfinished checkpoint
	n := 0
	cur := st
	var pending *state // a checkpoint being read, not yet complete
	for {
		t, body, size, err := readRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Whatever was read of an unfinished checkpoint is dropped, and
			// the file is cut back to where it began.
			return cur, good, n, err
		}
		switch {
		case t == recCheckpointBegin:
			if good != 0 {
				return cur, good, n, corrupt(t, "a checkpoint must start its segment")
			}
			pending = newState()
			if err := pending.apply(t, body); err != nil {
				return cur, good, n, err
			}
		case pending != nil:
			if err := pending.apply(t, body); err != nil {
				return cur, good, n, err
			}
			if t == recCheckpointEnd {
				cur, pending = pending, nil
			}
		default:
			if err := cur.apply(t, body); err != nil {
				return cur, good, n, err
			}
		}
		n++
		offset += int64(size)
		if pending == nil {
			good = offset
		}
	}
	if pending != nil {
		// The file ends inside a checkpoint: a compaction cut short.
		return cur, good, n, errTorn
	}
	return cur, good, n, nil
}

// openActive opens the newest segment for appending, creating the first one
// in an empty log.
func (fs *files) openActive() error {
	if len(fs.segs) == 0 {
		return fs.startSegment(0)
	}
	last := fs.segs[len(fs.segs)-1]
	f, err := os.OpenFile(last.path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	fs.active = f
	return nil
}

// startSegment creates a new, empty newest segment at log offset base.
func (fs *files) startSegment(base int64) error {
	path := filepath.Join(fs.dir, segmentName(base))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	// The new file's name must survive a crash too, or records written to it
	// could vanish with it: fsync the directory.
	if err := fs.syncDir(); err != nil {
		f.Close()
		return err
	}
	if fs.active != nil {
		fs.active.Close()
	}
	fs.active = f
	fs.segs = append(fs.segs, segment{base: base, path: path})
	return nil
}

func (fs *files) newest() *segment { return &fs.segs[len(fs.segs)-1] }

// end is the log offset just past the last byte written.
func (fs *files) end() int64 { s := fs.newest(); return s.base + s.size }

// size is the bytes in every segment.
func (fs *files) size() int64 {
	var n int64
	for _, s := range fs.segs {
		n += s.size
	}
	return n
}

// write appends bytes to the newest segment. They're not safe until sync.
func (fs *files) write(b []byte) error {
	n, err := fs.active.Write(b)
	fs.newest().size += int64(n)
	return err
}

// sync makes everything written so far durable. On macOS a plain fsync only
// hands the data to the drive, which may hold it in its cache through a power
// cut; Go's File.Sync asks for F_FULLFSYNC there, which also flushes the
// drive. fullSync false uses the plain fsync, to compare with Postgres's
// default on macOS (wal_sync_method = open_datasync), which doesn't flush it.
func (fs *files) sync() error {
	if fs.fullSync {
		return fs.active.Sync()
	}
	return syscall.Fsync(int(fs.active.Fd()))
}

func (fs *files) syncDir() error {
	d, err := os.Open(fs.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// dropBefore deletes every segment older than the newest.
func (fs *files) dropBefore() error {
	for _, s := range fs.segs[:len(fs.segs)-1] {
		if err := os.Remove(s.path); err != nil {
			return err
		}
	}
	fs.segs = fs.segs[len(fs.segs)-1:]
	return fs.syncDir()
}

func (fs *files) close() error {
	if fs.active == nil {
		return nil
	}
	err := fs.active.Close()
	fs.active = nil
	return err
}
