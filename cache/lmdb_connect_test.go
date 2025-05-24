package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestLMDBCacheWithStream(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "db")
	require.NoError(t, os.MkdirAll(dbDir, 0o755))

	env := service.NewEnvironment()
	b := env.NewStreamBuilder()

	require.NoError(t, b.AddCacheYAML(fmt.Sprintf(`label: lcache
lmdb:
  path: %s
  map_size: 1048576
`, dbDir)))

	prod, err := b.AddProducerFunc()
	require.NoError(t, err)

	var (
		resMu  sync.Mutex
		result []byte
	)
	require.NoError(t, b.AddConsumerFunc(func(ctx context.Context, m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}
		resMu.Lock()
		result = append([]byte(nil), b...)
		resMu.Unlock()
		return nil
	}))

	require.NoError(t, b.AddProcessorYAML(`cache:
  resource: lcache
  operator: set
  key: foo
  value: bar
`))
	require.NoError(t, b.AddProcessorYAML(`cache:
  resource: lcache
  operator: get
  key: foo
`))

	strm, err := b.Build()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, done := context.WithTimeout(t.Context(), 5*time.Second)
		defer done()

		require.NoError(t, prod(ctx, service.NewMessage(nil)))
		require.NoError(t, strm.StopWithin(time.Second))
	}()
	require.NoError(t, strm.Run(t.Context()))
	wg.Wait()

	resMu.Lock()
	defer resMu.Unlock()
	require.Equal(t, []byte("bar"), result)
}
