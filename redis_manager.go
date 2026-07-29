package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type managedRedis struct {
	config connectionConfig
	client redis.UniversalClient
}

type redisManager struct {
	mu          sync.RWMutex
	connections map[string]*managedRedis
	order       []string
}

func newRedisManager(config appConfig) (*redisManager, error) {
	manager := &redisManager{connections: make(map[string]*managedRedis, len(config.Connections))}
	for _, connection := range config.Connections {
		tlsConfig, err := connection.tlsConfig()
		if err != nil {
			return nil, fmt.Errorf("configure TLS for %s: %w", connection.ID, err)
		}
		password, err := connection.password()
		if err != nil {
			return nil, fmt.Errorf("read Redis password for %s: %w", connection.ID, err)
		}
		options := &redis.UniversalOptions{
			Addrs:        connection.Addrs,
			MasterName:   connection.MasterName,
			Username:     connection.Username,
			Password:     password,
			DB:           connection.DB,
			TLSConfig:    tlsConfig,
			PoolSize:     4,
			MinIdleConns: 1,
			MaxIdleConns: 2,
			PoolTimeout:  3 * time.Second,
			DialTimeout:  3 * time.Second,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
			MaxRetries:   1,
		}
		client := redis.NewUniversalClient(options)
		manager.connections[connection.ID] = &managedRedis{config: connection, client: client}
		manager.order = append(manager.order, connection.ID)
	}
	return manager, nil
}

func (m *redisManager) get(id string) (*managedRedis, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if id == "" && len(m.order) > 0 {
		id = m.order[0]
	}
	connection, ok := m.connections[id]
	if !ok {
		return nil, errors.New("unknown Redis connection")
	}
	return connection, nil
}

func (m *redisManager) ids() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]string(nil), m.order...)
}

func (m *redisManager) reload(config appConfig) error {
	next, err := newRedisManager(config)
	if err != nil {
		return err
	}
	m.mu.Lock()
	previous := m.connections
	m.connections = next.connections
	m.order = next.order
	m.mu.Unlock()
	go func() {
		time.Sleep(5 * time.Second)
		for _, connection := range previous {
			_ = connection.client.Close()
		}
	}()
	return nil
}

func (m *redisManager) close() {
	m.mu.Lock()
	connections := m.connections
	m.connections = make(map[string]*managedRedis)
	m.order = nil
	m.mu.Unlock()
	for _, connection := range connections {
		_ = connection.client.Close()
	}
}

type tailSubscription struct {
	Events <-chan tailEvent
	Cancel func()
}

type tailEvent struct {
	ID      string
	Payload []byte
}

type tailWorker struct {
	connection *managedRedis
	stream     string
	lastID     string
	mu         sync.Mutex
	next       int
	subs       map[int]chan tailEvent
	cancel     context.CancelFunc
}

type tailBroker struct {
	mu      sync.Mutex
	workers map[string]*tailWorker
	max     int
}

func newTailBroker(max int) *tailBroker {
	return &tailBroker{workers: make(map[string]*tailWorker), max: max}
}

func (b *tailBroker) subscribe(connection *managedRedis, stream, lastID string) (tailSubscription, error) {
	key := connection.config.ID + "\x00" + stream
	b.mu.Lock()
	worker := b.workers[key]
	if worker == nil {
		if len(b.workers) >= b.max {
			b.mu.Unlock()
			return tailSubscription{}, errors.New("live tail limit reached")
		}
		ctx, cancel := context.WithCancel(context.Background())
		worker = &tailWorker{connection: connection, stream: stream, lastID: lastID, subs: make(map[int]chan tailEvent), cancel: cancel}
		b.workers[key] = worker
		go b.run(ctx, key, worker)
	}
	worker.mu.Lock()
	id := worker.next
	worker.next++
	channel := make(chan tailEvent, 16)
	worker.subs[id] = channel
	worker.mu.Unlock()
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			worker.mu.Lock()
			delete(worker.subs, id)
			close(channel)
			empty := len(worker.subs) == 0
			if empty && b.workers[key] == worker {
				delete(b.workers, key)
			}
			worker.mu.Unlock()
			if empty {
				worker.cancel()
			}
			b.mu.Unlock()
		})
	}
	return tailSubscription{Events: channel, Cancel: cancel}, nil
}

func (b *tailBroker) run(ctx context.Context, key string, worker *tailWorker) {
	defer func() {
		b.mu.Lock()
		if b.workers[key] == worker {
			delete(b.workers, key)
		}
		b.mu.Unlock()
	}()
	for {
		result, err := worker.connection.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{worker.stream, worker.lastID},
			Count:   100,
			Block:   5 * time.Second,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(time.Second)
			continue
		}
		for _, stream := range result {
			for _, message := range stream.Messages {
				worker.lastID = message.ID
				event := tailEvent{
					ID:      message.ID,
					Payload: mustJSON(map[string]any{"id": message.ID, "fields": message.Values}),
				}
				worker.mu.Lock()
				for _, subscriber := range worker.subs {
					select {
					case subscriber <- event:
					default:
					}
				}
				worker.mu.Unlock()
			}
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (b *tailBroker) reset() {
	b.mu.Lock()
	workers := b.workers
	b.workers = make(map[string]*tailWorker)
	b.mu.Unlock()
	for _, worker := range workers {
		worker.cancel()
	}
}
