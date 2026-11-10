package log

/*
The store implementation knows how to write and read records using byte positions.
How do we find the byte position of a record without scanning the entire store file?
--------------------------------------------------------------------------------------------------
Store: Knows where the actual record bytes are stored.
Index: Knows where each record is located inside the store.
# mmap stands for Memory Mapping.
It is an operating-system feature that allows a process to map a file into
its virtual memory address space.
Index entry number	Logical record offset	Store byte position
    0						0						0
    1						1						13
    2						2						41
*/

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// index stores entries in format: [record offset][store byte position]
// the record offset is the logical, sequential position of the record in the segment.
// record offset occupies 4 bytes
// the store byte position occupies 8 bytes.
// index entry = 4 bytes offset + 8 bytes position = 12 bytes
var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// initialize the index
// find the existing file size
// resize the index file capacity to MaxIndexBytes
// reserve the maximum configured index capacity upfront
// create the memory mapping
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())
	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	if err != nil {
		return nil, err
	}
	idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

// sync changes from mmap to underlying file
func (i *index) Close() error {
	err := i.mmap.Sync(gommap.MS_SYNC)
	if err != nil {
		return err
	}

	err = i.file.Sync()
	if err != nil {
		return err
	}

	err = i.file.Truncate(int64(i.size))
	if err != nil {
		return err
	}
	return i.file.Close()
}

// input -> logical index position or entry number
// output -> (logical_offset, physical_store_position, error)
// first check if index is empty
// check for special value -1, -1 means give me last entry in the index
// calculate physical position inside index file
// check if the entry exists
// decode the logical offset
// decode the store position
func (i *index) Read(in int64) (uint32, uint64, error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	entry := uint32(in)
	if in == -1 {
		entry = uint32((i.size / entWidth) - 1)
	}
	pos := uint64(entry) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	off := enc.Uint32(i.mmap[pos : pos+offWidth])
	storePos := enc.Uint64(
		i.mmap[pos+offWidth : pos+entWidth],
	)
	return off, storePos, nil
}

// check if we have enough capacity
// write the offset position, 4 bytes for offset (example off = 2)
// write the store position, 8 bytes for store position (example pos = 40)
// i.mmap[24:28]->[00 00 00 02], i.mmap[28:36] -> [00 00 00 00 00 00 00 28]
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
