package cache

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestLMDBCacheSetGet(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")
	require.NoError(t, os.MkdirAll(dbDir, 0o755))
	conf, err := lmdbConfigSpec.ParseYAML("path: "+dbDir+"\nmap_size: 1048576", nil)
	require.NoError(t, err)

	cache, err := newLMDBCache(conf, nil)
	require.NoError(t, err)
	defer cache.Close(context.Background())

	require.NoError(t, cache.Set(context.Background(), "foo", []byte("bar"), nil))
	v, err := cache.Get(context.Background(), "foo")
	require.NoError(t, err)
	require.Equal(t, []byte("bar"), v)

	// Test Add prevents overwriting
	require.NoError(t, cache.Add(context.Background(), "baz", []byte("one"), nil))
	require.ErrorIs(t, cache.Add(context.Background(), "baz", []byte("two"), nil), service.ErrKeyAlreadyExists)
}

func TestLMDBCachePersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	require.NoError(t, os.MkdirAll(dbPath, 0o755))
	confYAML := "path: " + dbPath + "\nmap_size: 1048576"

	conf, err := lmdbConfigSpec.ParseYAML(confYAML, nil)
	require.NoError(t, err)

	c, err := newLMDBCache(conf, nil)
	require.NoError(t, err)

	require.NoError(t, c.Set(context.Background(), "a", []byte("b"), nil))
	require.NoError(t, c.Close(context.Background()))

	conf, err = lmdbConfigSpec.ParseYAML(confYAML, nil)
	require.NoError(t, err)
	c2, err := newLMDBCache(conf, nil)
	require.NoError(t, err)
	defer c2.Close(context.Background())

	v, err := c2.Get(context.Background(), "a")
	require.NoError(t, err)
	require.Equal(t, []byte("b"), v)
}
