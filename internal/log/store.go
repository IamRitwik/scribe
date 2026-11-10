package log

/*
1.Length-prefixed encoding: Every record starts with an 8-byte integer describing
the size of its payload.
2.Byte offsets: Records are identified by their starting positions in the file.
3.Buffered writing: Writes may initially remain in memory until Flush() is called.
4.Random access: ReadAt() lets us read from a particular file offset
without changing the shared file cursor.
5.Logical file size: s.size must account for both the record's length prefix and its payload.
Append	pos		n returned	Updated s.size
------------------------------------------
  1		0		  19			19
  2		19		  19			38
  3		38		  19			57
*/

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// get file metadata
// find current size of the file
// create the buffered writer
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// acquire the mutex
// remember the starting position (pos - starting byte offset of new record)
// write the length of the record first [8-byte length prefix] - encode integer into bytes
// write the actual record, w = number of payload bytes, [00 00 00 00 00 00 00 0B][hello world]
// calculate total bytes = length + payload
// update the size with total bytes
// return number of bytes appended, starting position of record, error if any
func (s *store) Append(p []byte) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos := s.size
	err := binary.Write(s.buf, enc, uint64(len(p)))
	if err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// add length prefix + record
	n := uint64(lenWidth) + uint64(w)
	s.size += n
	return n, pos, nil
}

// receive starting byte position of a record
// flush the buffer
// Write - writes into buffer, Flush - sends buffered data to file, Close - close the file handle
// read the length prefix, read 8 bytes starting at record's starting byte position
// decode the length enc.uint64(size) - decode bytes into integer
// read the actual record - pos + lenWidth
// Position 0  ─────────────── Length prefix (8 bytes)
// Position 8  ─────────────── Actual data (11 bytes)
// Position 19 ─────────────── Next record
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	_, err = s.File.ReadAt(size, int64(pos))
	if err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	_, err = s.File.ReadAt(b, int64(pos+lenWidth))
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
