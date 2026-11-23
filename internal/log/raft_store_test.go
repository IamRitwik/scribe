package log

import (
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/stretchr/testify/require"
)

func TestRaftStorePreservesIndexesAndSupportsReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")

	store, err := raftboltdb.NewBoltStore(path)
	require.NoError(t, err)

	// Verify the backend implements both Raft storage interfaces.
	var logStore raft.LogStore = store
	var stableStore raft.StableStore = store

	require.NotNil(t, logStore)
	require.NotNil(t, stableStore)

	// Store the original entries.
	original := []*raft.Log{
		{
			Index: 1,
			Term:  1,
			Type:  raft.LogCommand,
			Data:  []byte("one"),
		},
		{
			Index: 2,
			Term:  1,
			Type:  raft.LogCommand,
			Data:  []byte("two-old"),
		},
		{
			Index: 3,
			Term:  1,
			Type:  raft.LogCommand,
			Data:  []byte("three-old"),
		},
		{
			Index: 4,
			Term:  1,
			Type:  raft.LogCommand,
			Data:  []byte("four-old"),
		},
	}

	require.NoError(t, store.StoreLogs(original))

	// Delete the conflicting suffix.
	// DeleteRange is inclusive: indexes 2, 3, and 4 are deleted.
	require.NoError(t, store.DeleteRange(2, 4))

	// Store replacement entries at the same Raft indexes.
	replacement := []*raft.Log{
		{
			Index: 2,
			Term:  2,
			Type:  raft.LogCommand,
			Data:  []byte("two-new"),
		},
		{
			Index: 3,
			Term:  2,
			Type:  raft.LogCommand,
			Data:  []byte("three-new"),
		},
		{
			Index: 4,
			Term:  2,
			Type:  raft.LogCommand,
			Data:  []byte("four-new"),
		},
	}

	require.NoError(t, store.StoreLogs(replacement))

	// Verify index boundaries.
	first, err := store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)

	last, err := store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), last)

	// Verify that replacement entries were stored correctly.
	for _, expected := range replacement {
		var actual raft.Log

		require.NoError(t, store.GetLog(expected.Index, &actual))

		require.Equal(t, expected.Index, actual.Index)
		require.Equal(t, expected.Term, actual.Term)
		require.Equal(t, expected.Type, actual.Type)
		require.Equal(t, expected.Data, actual.Data)
	}

	// Verify that the entry before the deleted range remains intact.
	var firstEntry raft.Log

	require.NoError(t, store.GetLog(1, &firstEntry))
	require.Equal(t, []byte("one"), firstEntry.Data)

	// Close the database.
	require.NoError(t, store.Close())

	// Reopen it to verify persistence.
	reopened, err := raftboltdb.NewBoltStore(path)
	require.NoError(t, err)
	defer reopened.Close()

	var recovered raft.Log

	require.NoError(t, reopened.GetLog(3, &recovered))

	require.Equal(t, uint64(3), recovered.Index)
	require.Equal(t, uint64(2), recovered.Term)
	require.Equal(t, []byte("three-new"), recovered.Data)
}
