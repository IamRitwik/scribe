package log

import (
	"bytes"
	"testing"

	api "github.com/IamRitwik/scribe/api/v1"
)

func TestSegmentAppendAndRead(t *testing.T) {
	dir := t.TempDir()

	// Configure sufficiently large segment limits.
	var c Config
	c.Segment.MaxStoreBytes = 1024 * 1024
	c.Segment.MaxIndexBytes = 1024 * 1024

	// Create a new segment starting at logical offset 5.
	s, err := newSegment(dir, 5, c)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	// Append three records.
	values := [][]byte{
		[]byte("Hello"),
		[]byte("World"),
		[]byte("Scribe"),
	}

	for i, value := range values {
		record := &api.Record{
			Value: value,
		}

		offset, err := s.Append(record)
		if err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}

		expectedOffset := uint64(5 + i)
		if offset != expectedOffset {
			t.Fatalf("expected offset %d, got %d",
				expectedOffset, offset)
		}
	}

	// Read each record back using its logical offset.
	for i, expected := range values {
		offset := uint64(5 + i)

		record, err := s.Read(offset)
		if err != nil {
			t.Fatalf("failed to read offset %d: %v", offset, err)
		}

		if !bytes.Equal(record.Value, expected) {
			t.Fatalf("offset %d: expected %q, got %q",
				offset, expected, record.Value)
		}

		if record.Offset != offset {
			t.Fatalf("expected record offset %d, got %d",
				offset, record.Offset)
		}
	}

	// Close the segment.
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close segment: %v", err)
	}

	// Reopen the same segment.
	s, err = newSegment(dir, 5, c)
	if err != nil {
		t.Fatalf("failed to reopen segment: %v", err)
	}
	defer s.Close()

	// Verify that the next offset was restored.
	if s.nextOffset != 8 {
		t.Fatalf("expected nextOffset 8, got %d", s.nextOffset)
	}

	// Verify that existing data is still readable.
	record, err := s.Read(6)
	if err != nil {
		t.Fatalf("failed to read after reopening: %v", err)
	}

	if !bytes.Equal(record.Value, []byte("World")) {
		t.Fatalf("expected World, got %q", record.Value)
	}
}
