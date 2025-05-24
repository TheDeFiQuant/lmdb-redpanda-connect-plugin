package cache

import (
	"context"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var lmdbConfigSpec = service.NewConfigSpec().
	Summary("Stores key/value pairs within an embedded LMDB environment.").
	Field(service.NewStringField("path").Description("Directory path for the LMDB environment.")).
	Field(service.NewIntField("map_size").Description("Maximum size in bytes for the LMDB memory map.").Default(1 << 20))

func init() {
	if err := service.RegisterCache("lmdb", lmdbConfigSpec, newLMDBCache); err != nil {
		panic(err)
	}
}

type lmdbCache struct {
	env *lmdb.Env
	dbi lmdb.DBI
}

func newLMDBCache(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
	path, err := conf.FieldString("path")
	if err != nil {
		return nil, err
	}
	mapSize, err := conf.FieldInt("map_size")
	if err != nil {
		return nil, err
	}

	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	if err := env.SetMapSize(int64(mapSize)); err != nil {
		return nil, err
	}
	if err := env.SetMaxDBs(1); err != nil {
		return nil, err
	}
	if err := env.Open(path, 0, 0644); err != nil {
		return nil, err
	}

	var dbi lmdb.DBI
	if err := env.Update(func(txn *lmdb.Txn) error {
		var err error
		dbi, err = txn.OpenDBI("cache", lmdb.Create)
		return err
	}); err != nil {
		env.Close()
		return nil, err
	}

	return &lmdbCache{env: env, dbi: dbi}, nil
}

func (l *lmdbCache) Get(ctx context.Context, key string) ([]byte, error) {
	var res []byte
	err := l.env.View(func(txn *lmdb.Txn) error {
		v, err := txn.Get(l.dbi, []byte(key))
		if err != nil {
			if lmdb.IsNotFound(err) {
				return service.ErrKeyNotFound
			}
			return err
		}
		res = append([]byte(nil), v...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (l *lmdbCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return l.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(l.dbi, []byte(key), value, 0)
	})
}

func (l *lmdbCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	err := l.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(l.dbi, []byte(key), value, lmdb.NoOverwrite)
	})
	if lmdb.IsErrno(err, lmdb.KeyExist) {
		return service.ErrKeyAlreadyExists
	}
	return err
}

func (l *lmdbCache) Delete(ctx context.Context, key string) error {
	return l.env.Update(func(txn *lmdb.Txn) error {
		err := txn.Del(l.dbi, []byte(key), nil)
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	})
}

func (l *lmdbCache) Close(ctx context.Context) error {
	l.env.Close()
	return nil
}
