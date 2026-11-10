package log

/*
- Segment coordinates the store and index
- store file contains actual serialized records
- index file contains mapping from logical offsets to physical store positions
- Segment A: baseOffset 0, relative 2 → logical 2
- Segment B: baseOffset 5, relative 2 → logical 7
- Segment C: baseOffset 10, relative 2 → logical 12

{Logical Offset} = {Base Offset} + {Relative Offset}

{Relative Offset} = {Logical Offset} - {Base Offset}
*/

import (
	"fmt"
	"io"
	"os"
	"path"

	api "github.com/IamRitwik/scribe/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// open the store file, for baseOffset = 5 means 5.store
// open the index file, for baseOffset = 5 means 5.index
// figure out the next logical offset
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	s.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}

	off, _, err := s.index.Read(-1)
	if err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// assign a logical offset
// serialize the Record, convert protobuf record into bytes
// append to store
// write the mapping into index
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	err = s.index.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	)
	if err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}

// logical offset - which record is this in overall log (actual position in logical sequence)
// - 5, 6, 7, 8, 9 ...
// relative offset - which record is this inside this particular segment
// - 0, 1, 2, 3, 4 ...  starts with 0 in every segment
// physical store position - at which byte position in the store file does this record begin
// - 0, 25, 40, 100,75 ...
func (s *segment) Read(off uint64) (*api.Record, error) {
	if off < s.baseOffset || off >= s.nextOffset {
		return nil, io.EOF
	}
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	err := s.Close()
	if err != nil {
		return err
	}
	err = os.Remove(s.index.Name())
	if err != nil {
		return err
	}
	err = os.Remove(s.store.Name())
	if err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	err := s.index.Close()
	if err != nil {
		return err
	}
	err = s.store.Close()
	if err != nil {
		return err
	}
	return nil
}

func nearestMultiple(j uint64, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k

}

/*
Logical offset
    |
    | subtract baseOffset
    v
Relative offset
    |
    | look up in Index
    v
Physical Store position
    |
    | read from Store
    v
Serialized record
    |
    | proto.Unmarshal
    v
Original Protobuf record
*/
