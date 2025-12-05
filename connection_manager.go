package main

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ceph/go-ceph/rados"
)

var errConnectionUnavailable = errors.New("ceph connection unavailable")

type ConnectionManager struct {
	mu                sync.RWMutex
	conn              *rados.Conn
	config            CephConfig
	logger            *slog.Logger
	reconnecting      bool
	lastReconnectTime time.Time
	minReconnectDelay time.Duration
	maxReconnectDelay time.Duration
}

type CephConfig struct {
	PoolName    string
	KeyringPath string
	ClientID    string
	CephConf    string
}

func NewConnectionManager(config CephConfig, logger *slog.Logger) *ConnectionManager {
	cm := &ConnectionManager{
		config:            config,
		logger:            logger,
		minReconnectDelay: 1 * time.Second,
		maxReconnectDelay: 30 * time.Second,
	}

	if err := cm.connect(); err != nil {
		logger.Warn("initial ceph connection failed, will retry on first request", "error", err)
	} else {
		logger.Info("ceph connection established")
	}

	return cm
}

func (cm *ConnectionManager) connect() error {
	var conn *rados.Conn
	var err error

	if cm.config.ClientID != "" {
		conn, err = rados.NewConnWithUser(cm.config.ClientID)
	} else {
		conn, err = rados.NewConn()
	}
	if err != nil {
		return fmt.Errorf("failed to create RADOS connection: %w", err)
	}

	err = conn.ParseDefaultConfigEnv()
	if err != nil {
		return fmt.Errorf("failed to parse CEPH_ARGS: %w", err)
	}

	if cm.config.CephConf != "" {
		err = conn.ReadConfigFile(cm.config.CephConf)
	} else {
		err = conn.ReadDefaultConfigFile()
	}
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if cm.config.KeyringPath != "" {
		err = conn.SetConfigOption("keyring", cm.config.KeyringPath)
		if err != nil {
			return fmt.Errorf("failed to set keyring path: %w", err)
		}
	}

	err = conn.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to RADOS: %w", err)
	}

	cm.mu.Lock()
	oldConn := cm.conn
	cm.conn = conn
	cm.mu.Unlock()

	if oldConn != nil {
		oldConn.Shutdown()
	}

	return nil
}

func (cm *ConnectionManager) GetIOContext() (*rados.IOContext, error) {
	const maxAttempts = 2
	for attempt := 0; attempt < maxAttempts; attempt++ {
		cm.mu.RLock()
		conn := cm.conn
		cm.mu.RUnlock()

		if conn == nil {
			if err := cm.tryReconnect(); err != nil {
				return nil, errConnectionUnavailable
			}

			cm.mu.RLock()
			conn = cm.conn
			cm.mu.RUnlock()

			if conn == nil {
				return nil, errConnectionUnavailable
			}
		}

		ioctx, err := conn.OpenIOContext(cm.config.PoolName)
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				return nil, err
			}

			cm.logger.Error("failed to open IO context", "error", err, "attempt", attempt+1)
			cm.markConnectionBroken()
			if attempt < maxAttempts-1 {
				if err := cm.tryReconnect(); err != nil {
					return nil, errConnectionUnavailable
				}
				continue
			}
			return nil, errConnectionUnavailable
		}

		return ioctx, nil
	}

	return nil, errConnectionUnavailable
}

func (cm *ConnectionManager) GetConnection() (*rados.Conn, error) {
	cm.mu.RLock()
	conn := cm.conn
	cm.mu.RUnlock()

	if conn == nil {
		if err := cm.tryReconnect(); err != nil {
			return nil, errConnectionUnavailable
		}

		cm.mu.RLock()
		conn = cm.conn
		cm.mu.RUnlock()

		if conn == nil {
			return nil, errConnectionUnavailable
		}
	}

	return conn, nil
}

func (cm *ConnectionManager) markConnectionBroken() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.conn != nil {
		cm.conn.Shutdown()
		cm.conn = nil
	}
}

func (cm *ConnectionManager) tryReconnect() error {
	cm.mu.Lock()
	if cm.reconnecting {
		cm.mu.Unlock()
		return errConnectionUnavailable
	}

	now := time.Now()
	timeSinceLastReconnect := now.Sub(cm.lastReconnectTime)
	delay := cm.calculateBackoff(timeSinceLastReconnect)

	if timeSinceLastReconnect < delay {
		cm.mu.Unlock()
		return errConnectionUnavailable
	}

	cm.reconnecting = true
	cm.lastReconnectTime = now
	cm.mu.Unlock()

	defer func() {
		cm.mu.Lock()
		cm.reconnecting = false
		cm.mu.Unlock()
	}()

	cm.logger.Info("attempting to reconnect to ceph")
	if err := cm.connect(); err != nil {
		cm.logger.Warn("reconnection failed", "error", err)
		return err
	}

	cm.logger.Info("successfully reconnected to ceph")
	return nil
}

func (cm *ConnectionManager) calculateBackoff(timeSinceLastReconnect time.Duration) time.Duration {
	if timeSinceLastReconnect >= cm.maxReconnectDelay {
		return cm.minReconnectDelay
	}

	backoff := cm.minReconnectDelay
	for backoff < cm.maxReconnectDelay && backoff < timeSinceLastReconnect {
		backoff *= 2
	}

	if backoff > cm.maxReconnectDelay {
		backoff = cm.maxReconnectDelay
	}

	return backoff
}

func (cm *ConnectionManager) Shutdown() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.conn != nil {
		cm.conn.Shutdown()
		cm.conn = nil
	}
}
