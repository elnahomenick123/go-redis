package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClusterSlotRace(t *testing.T) {
	var masterAActiveCommands int32
	var masterBActiveCommands int32

	masterA := NewNodeClient("127.0.0.1:7001", func(ctx context.Context, cmd *Command) error {
		atomic.AddInt32(&masterAActiveCommands, 1)
		defer atomic.AddInt32(&masterAActiveCommands, -1)
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	masterB := NewNodeClient("127.0.0.1:7002", func(ctx context.Context, cmd *Command) error {
		atomic.AddInt32(&masterBActiveCommands, 1)
		defer atomic.AddInt32(&masterBActiveCommands, -1)
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	nodes := map[string]*NodeClient{
		"127.0.0.1:7001": masterA,
	}
	slots := make([]*NodeClient, 16384)
	for i := range slots {
		slots[i] = masterA
	}

	client := &ClusterClient{}
	client.state.Store(newClusterState(nodes, slots))

	var topologyMu sync.Mutex
	currentNodes := nodes
	currentSlots := slots

	client.getTopology = func() (map[string]*NodeClient, []*NodeClient, error) {
		topologyMu.Lock()
		defer topologyMu.Unlock()
		return currentNodes, currentSlots, nil
	}

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					cmd := &Command{Key: fmt.Sprintf("key-%d", workerID)}
					err := client.Process(context.Background(), cmd)
					if err != nil {
						t.Errorf("Worker %d failed to process command: %v", workerID, err)
					}
				}
			}
		}(i)
	}

	time.Sleep(100 * time.Millisecond)

	topologyMu.Lock()
	newNodes := map[string]*NodeClient{
		"127.0.0.1:7002": masterB,
	}
	newSlots := make([]*NodeClient, 16384)
	for i := range newSlots {
		newSlots[i] = masterB
	}
	currentNodes = newNodes
	currentSlots = newSlots
	topologyMu.Unlock()

	masterA.mu.Lock()
	masterA.onProcess = func(ctx context.Context, cmd *Command) error {
		return &MovedError{Slot: Slot(cmd.Key), Addr: "127.0.0.1:7002"}
	}
	masterA.mu.Unlock()

	time.Sleep(200 * time.Millisecond)

	close(stopChan)
	wg.Wait()

	if !masterA.IsClosed() {
		t.Errorf("Expected Master A to be closed")
	}
	if masterB.IsClosed() {
		t.Errorf("Expected Master B to not be closed")
	}

	if atomic.LoadInt64(&masterB.commandsExecuted) == 0 {
		t.Errorf("Expected commands to be executed on Master B after topology change")
	}
}

func TestClusterReloadCoalesce(t *testing.T) {
	var reloadCount int32
	client := &ClusterClient{}

	client.getTopology = func() (map[string]*NodeClient, []*NodeClient, error) {
		atomic.AddInt32(&reloadCount, 1)
		time.Sleep(50 * time.Millisecond)
		return map[string]*NodeClient{}, make([]*NodeClient, 16384), nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := client.reloadSlots()
			if err != nil {
				t.Errorf("reloadSlots failed: %v", err)
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&reloadCount) != 1 {
		t.Errorf("Expected exactly 1 reload, got %d", reloadCount)
	}
}
