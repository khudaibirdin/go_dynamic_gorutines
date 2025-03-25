package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	BUFFER_SIZE = 100
)

func handler(id string, data int) {
	log.Println(data)
}

func main() {
	buffer := make(chan int, BUFFER_SIZE)
	dw := DynamicWorkersPool{
		BufferConfig: BufferConfig{
			TopPoint:  90,
			DownPoint: 10,
		},
		WorkerConfig: WorkerConfig{
			Max: 100,
			Min: 5,
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go dw.WorkersRun(buffer, handler)

	wg.Add(1)
	go func() {
		for {
			select {
			case buffer <- 100:
				log.Print("Add")
			default:
				log.Print("Minus")
			}
			log.Printf("buffer size %d", len(buffer))
			time.Sleep(1 * time.Second)
		}
	}()
	wg.Wait()
}

type (
	DynamicWorkersPool struct {
		BufferConfig BufferConfig
		WorkerConfig WorkerConfig
		mu           sync.Mutex
		workers      []Worker
	}
	BufferConfig struct {
		TopPoint  int
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

// основной запуск динамического пула воркеров
func (dw *DynamicWorkersPool) WorkersRun(buffer chan int, handler func(id string, data int)) {
	var wg sync.WaitGroup
	for range dw.WorkerConfig.Min {
		wg.Add(1)
		dw.startWorker(buffer, handler)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		dw.mu.Lock()
		activeWorkers := len(dw.workers)
		dw.mu.Unlock()
		log.Printf("[WorkersRun] active workers = %d, buffer len = %d", activeWorkers, len(buffer))
		if len(buffer) >= dw.BufferConfig.TopPoint && activeWorkers < dw.WorkerConfig.Max {
			wg.Add(1)
			dw.startWorker(buffer, handler)
		} else if len(buffer) <= dw.BufferConfig.DownPoint && activeWorkers > dw.WorkerConfig.Min {
			dw.stopWorker()
			wg.Done()
		}
	}
}

// стартуем горутину
func (dw *DynamicWorkersPool) startWorker(buffer chan int, handler func(id string, data int)) {
	ctx, cancel := context.WithCancel(context.Background())
	worker := Worker{
		ctx:    ctx,
		cancel: cancel,
		id:     uuid.NewString()}
	dw.workers = append(dw.workers, worker)

	go func() {
		log.Printf("[startWorker] started worker %s", worker.id)
		for {
			select {
			case <-ctx.Done():
				log.Printf("[startWorker] stoped worker %s", worker.id)
				return
			case data := <-buffer:
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
	log.Printf("[startWorker] stoped worker %s", last.id)
}
