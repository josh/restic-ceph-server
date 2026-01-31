package main

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ceph/go-ceph/rados"
)

var errConnectionUnavailable = errors.New("ceph connection unavailable")

type ConnectionManager struct {
	mu                sync.RWMutex
	conn              *rados.Conn
	config            CephConfig
	reconnecting      bool
	lastReconnectTime time.Time
	minReconnectDelay time.Duration
	maxReconnectDelay time.Duration
	maxObjectSize     int64
	maxWriteSize      int64
	poolProperties    map[string]*PoolProperties
}

type CephConfig struct {
	PoolMapping   *PoolMapping
	KeyringPath   string
	ClientID      string
	CephConf      string
	MaxObjectSize int64
}

func NewConnectionManager(config CephConfig) *ConnectionManager {
	cm := &ConnectionManager{
		config:            config,
		minReconnectDelay: 1 * time.Second,
		maxReconnectDelay: 30 * time.Second,
	}

	if err := cm.connect(); err != nil {
		slog.Warn("initial ceph connection failed, will retry on first request", "error", err)
	} else {
		slog.Info("ceph connection established")
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

	success := false
	defer func() {
		if !success {
			conn.Shutdown()
		}
	}()

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

	success = true

	clusterMaxSize := int64(0)
	sizeStr, err := conn.GetConfigOption("osd_max_object_size")
	if err != nil {
		slog.Warn("failed to read osd_max_object_size from cluster", "error", err)
	} else {
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			slog.Warn("invalid osd_max_object_size value from cluster", "value", sizeStr, "error", err)
		} else if size <= 0 || size > math.MaxUint32 {
			slog.Warn("osd_max_object_size from cluster out of valid range", "value", size)
		} else {
			clusterMaxSize = size
			slog.Debug("loaded cluster max object size", "max_object_size", size)
		}
	}

	clusterMaxWriteSize := int64(0)
	writeSizeStr, err := conn.GetConfigOption("osd_max_write_size")
	if err != nil {
		slog.Warn("failed to read osd_max_write_size from cluster", "error", err)
	} else {
		writeSizeMB, err := strconv.ParseInt(writeSizeStr, 10, 64)
		if err != nil {
			slog.Warn("invalid osd_max_write_size value from cluster", "value", writeSizeStr, "error", err)
		} else if writeSizeMB <= 0 {
			slog.Warn("osd_max_write_size from cluster out of valid range", "value", writeSizeMB)
		} else {
			clusterMaxWriteSize = writeSizeMB * 1024 * 1024
			slog.Debug("loaded cluster max write size", "max_write_size", clusterMaxWriteSize)
		}
	}

	var maxSize int64
	if cm.config.MaxObjectSize > 0 {
		maxSize = cm.config.MaxObjectSize
		if clusterMaxSize > 0 && maxSize > clusterMaxSize {
			slog.Warn("configured max-object-size exceeds cluster limit, writes may fail",
				"configured", maxSize,
				"cluster_limit", clusterMaxSize)
		}
	} else if clusterMaxSize > 0 {
		maxSize = clusterMaxSize
	} else {
		maxSize = defaultMaxObjectSize
		slog.Warn("using default max object size", "default", maxSize)
	}

	var maxWriteSize int64
	if clusterMaxWriteSize > 0 {
		maxWriteSize = clusterMaxWriteSize
	} else {
		maxWriteSize = defaultMaxWriteSize
		slog.Warn("using default max write size", "default", maxWriteSize)
	}

	poolProps := make(map[string]*PoolProperties)
	for _, poolName := range cm.config.PoolMapping.Pools() {
		ioctx, err := conn.OpenIOContext(poolName)
		if err != nil {
			return fmt.Errorf("failed to open pool %q: %w", poolName, err)
		}

		props := &PoolProperties{
			RequiresAlignment: false,
			Alignment:         1,
		}

		if ra, err := ioctx.RequiresAlignment(); err == nil && ra {
			props.RequiresAlignment = true
			if align, err := ioctx.Alignment(); err == nil && align > 1 {
				props.Alignment = align
				slog.Debug("pool requires alignment", "pool", poolName, "alignment", align)
			}
		}

		poolProps[poolName] = props
		ioctx.Destroy()
	}

	cm.mu.Lock()
	oldConn := cm.conn
	cm.conn = conn
	cm.maxObjectSize = maxSize
	cm.maxWriteSize = maxWriteSize
	cm.poolProperties = poolProps
	cm.mu.Unlock()

	if oldConn != nil {
		oldConn.Shutdown()
	}

	return nil
}

func (cm *ConnectionManager) GetIOContextForPool(poolName string) (*rados.IOContext, error) {
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

		ioctx, err := conn.OpenIOContext(poolName)
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				return nil, err
			}

			slog.Error("failed to open IO context", "pool", poolName, "error", err, "attempt", attempt+1)
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

func (cm *ConnectionManager) GetIOContextForType(blobType BlobType) (*rados.IOContext, error) {
	poolName := cm.config.PoolMapping.GetPoolForType(blobType)
	return cm.GetIOContextForPool(poolName)
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

func (cm *ConnectionManager) GetMaxObjectSize() (int64, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.conn == nil {
		return 0, errConnectionUnavailable
	}

	return cm.maxObjectSize, nil
}

func (cm *ConnectionManager) GetMaxWriteSize() (int64, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.conn == nil {
		return 0, errConnectionUnavailable
	}

	return cm.maxWriteSize, nil
}

func (cm *ConnectionManager) GetPoolAlignment(poolName string) (bool, uint64, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.conn == nil {
		return false, 0, errConnectionUnavailable
	}

	props, ok := cm.poolProperties[poolName]
	if !ok {
		return false, 0, fmt.Errorf("pool %q not configured", poolName)
	}

	return props.RequiresAlignment, props.Alignment, nil
}

func (cm *ConnectionManager) ValidateBufferAlignment(bufferSize int64) error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.conn == nil {
		return errConnectionUnavailable
	}

	var misaligned []string
	for _, poolName := range cm.config.PoolMapping.Pools() {
		props := cm.poolProperties[poolName]
		if props.RequiresAlignment && props.Alignment > 1 {
			if bufferSize%int64(props.Alignment) != 0 {
				misaligned = append(misaligned, fmt.Sprintf("%s (requires %d)", poolName, props.Alignment))
			}
		}
	}

	if len(misaligned) > 0 {
		return fmt.Errorf("buffer size %d not aligned for pools: %s",
			bufferSize, strings.Join(misaligned, ", "))
	}

	return nil
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

	slog.Info("attempting to reconnect to ceph")
	if err := cm.connect(); err != nil {
		slog.Warn("reconnection failed", "error", err)
		return err
	}

	slog.Info("successfully reconnected to ceph")
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
