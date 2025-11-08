package main

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type idleMonitor struct {
	activeConns  atomic.Int32
	lastActivity atomic.Int64
	done         chan struct{}
	closeOnce    sync.Once
	maxIdleTime  time.Duration
}

func newIdleMonitor(maxIdleTime time.Duration) *idleMonitor {
	monitor := &idleMonitor{
		done:        make(chan struct{}),
		maxIdleTime: maxIdleTime,
	}
	monitor.lastActivity.Store(time.Now().UnixNano())

	go monitor.checkIdleTimeout()

	return monitor
}

func (im *idleMonitor) Incr() {
	im.activeConns.Add(1)
	im.lastActivity.Store(time.Now().UnixNano())
}

func (im *idleMonitor) Decr() {
	im.activeConns.Add(-1)
	im.lastActivity.Store(time.Now().UnixNano())
}

func (im *idleMonitor) Done() <-chan struct{} {
	return im.done
}

func (im *idleMonitor) Stop() {
	im.closeOnce.Do(func() {
		close(im.done)
	})
}

func (im *idleMonitor) checkIdleTimeout() {
	timer := time.NewTimer(im.maxIdleTime)
	defer timer.Stop()

	for {
		select {
		case <-im.done:
			return
		case <-timer.C:
			activeConns := im.activeConns.Load()
			lastActivityNano := im.lastActivity.Load()
			lastActivity := time.Unix(0, lastActivityNano)
			idleTime := time.Since(lastActivity)

			if activeConns == 0 && idleTime >= im.maxIdleTime {
				log.Printf("Server idle timeout reached (%v), shutting down\n", im.maxIdleTime)
				im.Stop()
				return
			}

			if activeConns == 0 {
				remaining := im.maxIdleTime - idleTime
				if remaining < 0 {
					remaining = 0
				}
				timer.Reset(remaining)
			} else {
				timer.Reset(im.maxIdleTime)
			}
		}
	}
}
