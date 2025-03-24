package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	BUFFER_SIZE = 100
)

func handler(id string, data int) {
	fmt.Println(data)
}

func main() {
	buffer := make(chan int, BUFFER_SIZE)
	dw := DynamicWorkersPool{
		BufferConfig: BufferConfig{
			TopPoint: 90,
			DownPoint: 10,
		},
		WorkerConfig: WorkerConfig{
			Max: 100,
			Min: 5,
		},
	}
	
	dw.WorkersRun(buffer, handler)

	go func() {
		for {
			buffer <- 100
			time.Sleep(1 * time.Second)
		}
	}()
}

type (
	DynamicWorkersPool struct {
		BufferConfig BufferConfig
		WorkerConfig WorkerConfig
		mu sync.Mutex
		workers []Worker
	}
	BufferConfig struct {
		TopPoint int
		DownPoint int
	}
	WorkerConfig struct {
		Max int
		Min int
	}
	Worker struct {
		ctx    context.Context
		cancel context.CancelFunc
		id     string
	}
)

func (dw *DynamicWorkersPool) WorkersRun(buffer chan int, handler func(id string, data int)) {
	for range dw.WorkerConfig.Min {
		dw.startWorker(buffer, handler)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		dw.mu.Lock()
		activeWorkers := len(dw.workers)
		dw.mu.Unlock()
		fmt.Printf("[WorkersRun] active workers = %d, buffer len = %d\n", activeWorkers, len(buffer))
		if len(buffer) >= dw.BufferConfig.TopPoint && activeWorkers < dw.WorkerConfig.Max {
			dw.startWorker(buffer, handler)
		} else if len(buffer) <= dw.BufferConfig.DownPoint && activeWorkers > dw.WorkerConfig.Min {
			dw.stopWorker()
		}
	}
}

// стартуем горутину
func (dw *DynamicWorkersPool) startWorker(buffer chan int, handler func(id string, data int)) {
	ctx, cancel := context.WithCancel(context.Background())
	worker := Worker{
		ctx: ctx,
		cancel: cancel,
		id: uuid.NewString()}
	dw.workers = append(dw.workers, worker)
	
	go func() {
		fmt.Printf("[startWorker] started worker %s\n", worker.id)
		for {
			select {
			case <-ctx.Done():
				return
			case data := <- buffer:
				handler(worker.id, data)
			}
		}
	}()

}

// останавливаем горутину
func (dw *DynamicWorkersPool) stopWorker() {
	if len(dw.workers) == 0 {
		return
	}
	last := dw.workers[len(dw.workers)-1]
	last.cancel()
	dw.workers = dw.workers[:len(dw.workers)-1]
	fmt.Printf("[startWorker] stoped worker %s\n", last.id)
}
