package log

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/IamRitwik/scribe/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

// END: intro

// START: append_read
func testAppendRead(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

// END: append_read

// START: out_of_range
func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
}

// END: out_of_range

// START: init_existing
func testInitExisting(t *testing.T, o *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := o.Append(append)
		require.NoError(t, err)
	}
	require.NoError(t, o.Close())

	off, err := o.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = o.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	n, err := NewLog(o.Dir, o.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

// END: init_existing

// START: reader
func testReader(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

// END: reader

// START: truncate
func testTruncate(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}

// END: truncate

func TestLogReadFromMiddleSegment(t *testing.T) {
	dir := t.TempDir()

	var c Config
	c.Segment.InitialOffset = 0

	// A tiny Store limit forces rotation after every append.
	c.Segment.MaxStoreBytes = 1
	c.Segment.MaxIndexBytes = 1024

	l, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	values := [][]byte{
		[]byte("CAT"),
		[]byte("DOG"),
		[]byte("BIRD"),
		[]byte("FISH"),
	}

	for i, value := range values {
		record := &api.Record{Value: value}

		offset, err := l.Append(record)
		if err != nil {
			t.Fatal(err)
		}

		if offset != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
	}

	if len(l.segments) < 3 {
		t.Fatalf("expected at least 3 segments, got %d", len(l.segments))
	}

	// Offset 2 belongs to a middle segment.
	got, err := l.Read(2)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got.Value, values[2]) {
		t.Fatalf("expected %q, got %q", values[2], got.Value)
	}

	if got.Offset != 2 {
		t.Fatalf("expected record offset 2, got %d", got.Offset)
	}
}

func TestLogAppendAcrossSegmentRotation(t *testing.T) {
	dir := t.TempDir()

	var c Config
	c.Segment.InitialOffset = 0
	c.Segment.MaxStoreBytes = 1
	c.Segment.MaxIndexBytes = 1024

	l, err := NewLog(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	const count = 10

	for i := 0; i < count; i++ {
		record := &api.Record{
			Value: fmt.Appendf(nil, "record-%d", i),
		}

		offset, err := l.Append(record)
		if err != nil {
			t.Fatal(err)
		}

		expected := uint64(i)
		if offset != expected {
			t.Fatalf(
				"append %d: expected offset %d, got %d",
				i, expected, offset,
			)
		}
	}

	for i := 0; i < count; i++ {
		record, err := l.Read(uint64(i))
		if err != nil {
			t.Fatalf("read offset %d: %v", i, err)
		}

		expected := fmt.Sprintf("record-%d", i)
		if string(record.Value) != expected {
			t.Fatalf(
				"offset %d: expected %q, got %q",
				i, expected, record.Value,
			)
		}

		if record.Offset != uint64(i) {
			t.Fatalf(
				"offset %d: record contains offset %d",
				i, record.Offset,
			)
		}
	}
}
