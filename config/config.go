package config

import (
	"errors"
	"sync"
	"time"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/config/source"
	"github.com/micro/go-micro/config/reader"
	"github.com/micro/go-micro/config/reader/json"

	proto "github.com/microhq/config-srv/proto/config"
	"golang.org/x/net/context"
)

var (
	// We need a path splitter since its structured in go-os
	PathSplitter = "/"
	WatchTopic   = "micro.config.watch"

	read   reader.Reader
	mtx      sync.RWMutex
	watchers = make(map[string][]*watcher)
)

type watcher struct {
	id   string
	exit chan bool
	next chan *proto.WatchResponse
}

func (w *watcher) Next() (*proto.WatchResponse, error) {
	select {
	case c := <-w.next:
		return c, nil
	case <-w.exit:
		return nil, errors.New("watcher stopped")
	}
}

func (w *watcher) Stop() error {
	select {
	case <-w.exit:
		return errors.New("already stopped")
	default:
		close(w.exit)
	}

	mtx.Lock()
	var wslice []*watcher

	for _, watch := range watchers[w.id] {
		if watch != w {
			wslice = append(wslice, watch)
		}
	}

	watchers[w.id] = wslice
	mtx.Unlock()

	return nil
}

func Init() error {
	read = json.NewReader()
	return nil
}

func Parse(ch ...*source.ChangeSet) (*source.ChangeSet, error) {
	return read.Merge(ch...)
}

func Values(ch *source.ChangeSet) (reader.Values, error) {
	return read.Values(ch)
}

// Watch created by a client RPC request
func Watch(id string) (*watcher, error) {
	mtx.Lock()
	w := &watcher{
		id:   id,
		exit: make(chan bool),
		next: make(chan *proto.WatchResponse),
	}
	watchers[id] = append(watchers[id], w)
	mtx.Unlock()
	return w, nil

}

// Used as a subscriber between config services for events
func Watcher(ctx context.Context, ch *proto.WatchResponse) error {
	mtx.RLock()
	for _, sub := range watchers[ch.Id] {
		select {
		case sub.next <- ch:
		case <-time.After(time.Millisecond * 100):
		}
	}
	mtx.RUnlock()
	return nil
}

// Publish a change
func Publish(ctx context.Context, ch *proto.WatchResponse) error {
	req := client.NewMessage(WatchTopic, ch)
	return client.Publish(ctx, req)
}
