package log

/*
Logical offset: position of a record in the complete log.

Relative offset: position within its particular segment.

Store position: physical byte position inside that segment's .store file.
*/

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/IamRitwik/scribe/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment   // segment currently receiving new records
	segments      []*segment // entire log
}

// supply defaults if the segment limits are zero
// create the log object and calls setup()
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	log := &Log{
		Dir:    dir,
		Config: c,
	}
	return log, log.setup()
}

// reconstructs the log from disk
// read the directory and extracts filenames without extension
// parses the base offset values and sorts them [0, 0, 1000, 1000, 2000, 2000...]
// create each segment
func (l *Log) setup() error {
	if err := os.MkdirAll(l.Dir, 0755); err != nil {
		return err
	}
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		ext := path.Ext(file.Name())
		if ext != ".store" && ext != ".index" {
			continue
		}
		offStr := strings.TrimSuffix(file.Name(), ext)
		off, err := strconv.ParseUint(offStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid segment filename %q: %w", file.Name(), err)
		}

		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i int, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		err = l.newSegment(baseOffsets[i])
		if err != nil {
			return err
		}
		// baseOffset contains duplicate for index and store so we skip the dup
		i++
	}
	if l.segments == nil {
		err = l.newSegment(l.Config.Segment.InitialOffset)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// if active segment is already full, rotate before append
	if l.activeSegment.IsMaxed() {
		err := l.newSegment(l.activeSegment.nextOffset)
		if err != nil {
			return 0, err
		}
	}

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	// rotate after append if the record filled the active segment
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
		if err != nil {
			return 0, err
		}
	}
	return off, nil
}

// identify the segment owing the offset
// record, err := log.Read(3) -> baseOffset = 2, nextOffset = 4 -> true
// delegate to appropriate segment for read
// calculate relative offset for the index
// index returns store position
// store reads length prefix and payload from byte position
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}
	return s.Read(off)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	l.segments = nil
	l.activeSegment = nil
	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.segments) == 0 {
		return 0, fmt.Errorf("log has no segments!")
	}

	last := l.segments[len(l.segments)-1]

	if last.nextOffset == last.baseOffset {
		return 0, io.EOF
	}
	return last.nextOffset - 1, nil
}

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			err := s.Remove()
			if err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
