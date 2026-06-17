package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

type NodeClient struct {
	addr             string
	closed           int32
	refCount         int32
	commandsExecuted int64

	mu        sync.Mutex
	isClosed  bool
	closeOnce sync.Once
	onProcess func(ctx context.Context, cmd *Command) error
}

func NewNodeClient(addr string, onProcess func(ctx context.Context, cmd *Command) error) *NodeClient {
	return &NodeClient{
		addr:      addr,
		onProcess: onProcess,
	}
}

func (n *NodeClient) IncRef() {
	atomic.AddInt32(&n.refCount, 1)
}

func (n *NodeClient) DecRef() {
	if atomic.AddInt32(&n.refCount, -1) == 0 {
		if atomic.LoadInt32(&n.closed) == 1 {
			n.realClose()
		}
	}
}

func (n *NodeClient) Close() error {
	if atomic.CompareAndSwapInt32(&n.closed, 0, 1) {
		if atomic.LoadInt32(&n.refCount) == 0 {
			n.realClose()
		}
	}
	return nil
}

func (n *NodeClient) realClose() {
	n.closeOnce.Do(func() {
		n.mu.Lock()
		n.isClosed = true
		n.mu.Unlock()
	})
}

func (n *NodeClient) IsClosed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.isClosed
}

func (n *NodeClient) Process(ctx context.Context, cmd *Command) error {
	n.mu.Lock()
	closed := n.isClosed
	n.mu.Unlock()
	if closed {
		return errors.New("redis: connection pool is closed")
	}
	atomic.AddInt64(&n.commandsExecuted, 1)
	if n.onProcess != nil {
		return n.onProcess(ctx, cmd)
	}
	return nil
}

type clusterState struct {
	nodes map[string]*NodeClient
	slots []*NodeClient
}

func newClusterState(nodes map[string]*NodeClient, slots []*NodeClient) *clusterState {
	return &clusterState{
		nodes: nodes,
		slots: slots,
	}
}
