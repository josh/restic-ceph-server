package main

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/ceph/go-ceph/rados"
)

var (
	errConnectionUnavailable = errors.New("ceph connection unavailable")
	errPoolNotConfigured     = errors.New("pool not configured for blob type")
	errRepoNotInitialized    = errors.New("repository not initialized")
)

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

	serverConfig     *ServerConfig
	serverConfigOnce sync.Once
	serverConfigErr  error
}

type CephConfig struct {
	ConfigPoolName string
	KeyringPath    string
	ClientID       string
	CephConf       string
	MaxObjectSize  int64
}

func NewConnectionManager(config CephConfig) *ConnectionManager {
	cm := &ConnectionManager{
		config:            config,
		minReconnectDelay: 1 * time.Second,
		maxReconnectDelay: 30 * time.Second,
		poolProperties:    make(map[string]*PoolProperties),
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

	cm.mu.Lock()
	oldConn := cm.conn
	cm.conn = conn
	cm.maxObjectSize = maxSize
	cm.maxWriteSize = maxWriteSize
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

func (cm *ConnectionManager) GetIOContextForType(blobType BlobType) (*rados.IOContext, string, error) {
	if blobType == BlobTypeConfig {
		ioctx, err := cm.GetIOContextForPool(cm.config.ConfigPoolName)
		return ioctx, cm.config.ConfigPoolName, err
	}

	sc, err := cm.getServerConfig()
	if err != nil {
		return nil, "", err
	}
	if sc == nil {
		return nil, "", errRepoNotInitialized
	}

	poolName := sc.Pools.GetPoolForType(blobType)
	if poolName == "" {
		return nil, "", fmt.Errorf("%w: %s", errPoolNotConfigured, blobType)
	}

	ioctx, err := cm.GetIOContextForPool(poolName)
	return ioctx, poolName, err
}

func (cm *ConnectionManager) getServerConfig() (*ServerConfig, error) {
	cm.serverConfigOnce.Do(func() {
		conn, err := cm.GetConnection()
		if err != nil {
			cm.serverConfigErr = err
			return
		}

		sc, err := LoadServerConfig(conn, cm.config.ConfigPoolName)
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				cm.serverConfig = nil
				cm.serverConfigErr = nil
				return
			}
			cm.serverConfigErr = err
			return
		}

		if sc != nil {
			slog.Info("loaded server-config", "version", sc.Version,
				"striper_enabled", sc.StriperEnabled)

			cm.mu.Lock()
			cm.poolProperties = probePoolProperties(conn, sc.Pools.UniquePools())
			cm.mu.Unlock()
		}

		cm.serverConfig = sc
	})

	return cm.serverConfig, cm.serverConfigErr
}

func (cm *ConnectionManager) InvalidateServerConfig() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.serverConfigOnce = sync.Once{}
	cm.serverConfig = nil
	cm.serverConfigErr = nil
}

func (cm *ConnectionManager) GetStriperEnabled() (bool, error) {
	sc, err := cm.getServerConfig()
	if err != nil {
		return false, err
	}
	if sc == nil {
		return false, errRepoNotInitialized
	}
	return sc.StriperEnabled, nil
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
		return false, 1, nil
	}

	return props.RequiresAlignment, props.Alignment, nil
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

func probePoolProperties(conn *rados.Conn, poolNames []string) map[string]*PoolProperties {
	poolProps := make(map[string]*PoolProperties)
	for _, poolName := range poolNames {
		ioctx, err := conn.OpenIOContext(poolName)
		if err != nil {
			if errors.Is(err, rados.ErrNotFound) {
				slog.Debug("pool does not exist, skipping property probe", "pool", poolName)
			} else {
				slog.Warn("failed to probe pool properties", "pool", poolName, "error", err)
			}
			continue
		}

		props := &PoolProperties{RequiresAlignment: false, Alignment: 1}
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
	return poolProps
}

func (cm *ConnectionManager) Shutdown() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.conn != nil {
		cm.conn.Shutdown()
		cm.conn = nil
	}
}
