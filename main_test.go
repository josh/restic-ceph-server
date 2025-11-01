package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
)

func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"restic-ceph-server": main,
	})
}

const timeoutGracePeriod = 2 * time.Second

func TestScript(t *testing.T) {
	ctx := t.Context()
	var cancel context.CancelFunc

	var deadline time.Time

	if dl, ok := t.Deadline(); ok {
		if time.Until(dl) <= timeoutGracePeriod {
			t.Fatalf("not enough time")
		}
		deadline = dl.Add(-timeoutGracePeriod)
		ctx, cancel = context.WithDeadline(ctx, deadline)
		t.Cleanup(cancel)
	}

	bufferedLog := &BufferedLog{target: os.Stderr}
	confPath, err := startCephCluster(t, ctx, bufferedLog)
	if err != nil {
		t.Log("\n=== Ceph cluster setup logs ===")
		if flushErr := bufferedLog.Flush(); flushErr != nil {
			t.Logf("failed to flush setup log: %v", flushErr)
		}
		t.Fatal(err)
	}

	bufferedLog.EnableStream()

	updateScripts, _ := strconv.ParseBool(os.Getenv("UPDATE_SCRIPTS"))

	testscript.Run(t, testscript.Params{
		Dir:             "testdata",
		ContinueOnError: true,
		UpdateScripts:   updateScripts,
		Deadline:        deadline,
		Setup: func(env *testscript.Env) error {
			poolName, err := createCephPool(ctx, confPath)
			if err != nil {
				return err
			}

			logFile, err := touchServerLog(t, t.TempDir())
			if err != nil {
				return err
			}
			go tailServerLog(t, ctx, logFile)
			env.Setenv("__RESTIC_CEPH_SERVER_LOG_FILE", logFile)
			env.Setenv("__RESTIC_CEPH_VERBOSE", "1")

			if !deadline.IsZero() {
				env.Setenv("__RESTIC_CEPH_SERVER_DEADLINE", deadline.Format(time.RFC3339))
			}
			env.Setenv("CEPH_CONF", confPath)
			env.Setenv("CEPH_POOL", poolName)
			env.Setenv("RESTIC_CACHE_DIR", filepath.Join(t.TempDir(), "restic-cache"))

			port, err := getFreePort()
			if err != nil {
				return fmt.Errorf("failed to allocate PORT: %w", err)
			}
			env.Setenv("PORT", strconv.Itoa(port))

			return nil
		},
	})
}

func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = listener.Close() }()

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

func touchServerLog(t *testing.T, tmpDir string) (string, error) {
	t.Helper()

	logFile := filepath.Join(tmpDir, "server.log")

	file, err := os.Create(logFile)
	if err != nil {
		return "", fmt.Errorf("failed to create log file: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("failed to close log file: %w", err)
	}

	return logFile, nil
}

func tailServerLog(t *testing.T, ctx context.Context, logFile string) {
	t.Helper()

	f, err := os.Open(logFile)
	if err != nil {
		t.Errorf("failed to open log file for reading: %v", err)
		return
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("failed to close log file: %v", err)
		}
	}()

	reader := bufio.NewReader(f)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						break
					}
					t.Errorf("failed to read from log file: %v", err)
					return
				}
				line = strings.TrimSuffix(line, "\n")
				t.Logf("[restic-ceph-server] %s", line)
			}
		}
	}
}

func startCephCluster(t *testing.T, ctx context.Context, out io.Writer) (string, error) {
	t.Helper()

	startupCtx, startupCancel := context.WithTimeout(ctx, 10*time.Second)
	defer startupCancel()

	confPath, err := setupCephDir(startupCtx, t.TempDir(), out)
	if err != nil {
		return "", err
	}

	if err := startCephMon(t, ctx, startupCtx, confPath, out); err != nil {
		return "", err
	}

	if err := startCephOsd(t, ctx, startupCtx, confPath, out); err != nil {
		return "", err
	}

	return confPath, nil
}

func setupCephDir(ctx context.Context, tmpDir string, out io.Writer) (string, error) {
	fsid := "6bb5784d-86b1-4b48-aff7-04d5dd22ef07"
	confPath := filepath.Join(tmpDir, "ceph.conf")

	cephConfig := map[string]map[string]string{
		"global": {
			"fsid":                                  fsid,
			"mon_host":                              "v1:127.0.0.1:6789/0",
			"public_network":                        "127.0.0.1/32",
			"auth_cluster_required":                 "none",
			"auth_service_required":                 "none",
			"auth_client_required":                  "none",
			"auth_allow_insecure_global_id_reclaim": "true",
			"pid_file":                              filepath.Join(tmpDir, "$type.$id.pid"),
			"admin_socket":                          filepath.Join(tmpDir, "$name.$pid.asok"),
			"keyring":                               "/dev/null",
			"log_to_file":                           "false",
			"log_to_stderr":                         "true",
			"osd_pool_default_size":                 "1",
			"osd_pool_default_min_size":             "1",
			"osd_crush_chooseleaf_type":             "0",
			"mon_allow_pool_size_one":               "true",
		},
		"mon": {
			"mon_initial_members":       "mon1",
			"mon_data":                  filepath.Join(tmpDir, "mon", "ceph-$id"),
			"mon_cluster_log_to_file":   "false",
			"mon_cluster_log_to_stderr": "true",
			"mon_allow_pool_delete":     "true",
		},
		"osd": {
			"osd_data":        filepath.Join(tmpDir, "osd", "ceph-$id"),
			"osd_objectstore": "memstore",
		},
	}

	err := os.MkdirAll(filepath.Join(tmpDir, "mon"), 0o755)
	if err != nil {
		return confPath, err
	}

	err = os.MkdirAll(filepath.Join(tmpDir, "osd", "ceph-0"), 0o755)
	if err != nil {
		return confPath, err
	}

	confContent := generateINIConfig(cephConfig)
	err = os.WriteFile(confPath, []byte(confContent), 0o644)
	if err != nil {
		return confPath, err
	}

	monmapPath := filepath.Join(tmpDir, "monmap")
	cmd := exec.CommandContext(ctx, "monmaptool", "--conf", confPath, monmapPath, "--create", "--fsid", fsid)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return confPath, fmt.Errorf("failed to create monitor map: %w", err)
	}

	cmd = exec.CommandContext(ctx, "monmaptool", "--conf", confPath, monmapPath, "--add", "mon1", "127.0.0.1:6789")
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return confPath, fmt.Errorf("failed to add monitor to map: %w", err)
	}

	cmd = exec.CommandContext(ctx, "ceph-mon", "--conf", confPath, "--mkfs", "--id", "mon1", "--monmap", monmapPath)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return confPath, fmt.Errorf("failed to initialize monitor filesystem: %w", err)
	}

	err = os.Remove(monmapPath)
	if err != nil {
		return confPath, err
	}

	return confPath, nil
}

func generateINIConfig(config map[string]map[string]string) string {
	var result strings.Builder

	sections := make([]string, 0, len(config))
	for section := range config {
		sections = append(sections, section)
	}
	sort.Strings(sections)

	for i, section := range sections {
		if i > 0 {
			result.WriteString("\n")
		}
		result.WriteString(fmt.Sprintf("[%s]\n", section))

		keys := make([]string, 0, len(config[section]))
		for key := range config[section] {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			result.WriteString(fmt.Sprintf("%s = %s\n", key, config[section][key]))
		}
	}

	return result.String()
}

func startCephMon(t *testing.T, ctx context.Context, startupCtx context.Context, confPath string, out io.Writer) error {
	t.Helper()
	cmd := exec.CommandContext(ctx, "ceph-mon", "--conf", confPath, "--id", "mon1", "--foreground")
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to spawn ceph-mon: %w", err)
	}

	t.Cleanup(func() {
		if err := cmd.Wait(); err != nil {
			t.Logf("ceph-mon exited with error: %v", err)
		}
	})

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-startupCtx.Done():
			return startupCtx.Err()
		case <-ticker.C:
			if status, err := checkCephStatus(startupCtx, confPath); err == nil && status.Monmap.NumMons > 0 {
				return nil
			}
		}
	}
}

func startCephOsd(t *testing.T, ctx context.Context, startupCtx context.Context, confPath string, out io.Writer) error {
	t.Helper()
	cmd := exec.CommandContext(ctx, "ceph-osd", "--conf", confPath, "--id", "0", "--mkfs")
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to initialize OSD filesystem: %w", err)
	}

	cmd = exec.CommandContext(ctx, "ceph-osd", "--conf", confPath, "--id", "0", "--foreground")
	cmd.Stdout = out
	cmd.Stderr = out

	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start OSD: %w", err)
	}

	t.Cleanup(func() {
		if err := cmd.Wait(); err != nil {
			t.Logf("ceph-osd exited with error: %v", err)
		}
	})

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-startupCtx.Done():
			return startupCtx.Err()
		case <-ticker.C:
			if status, err := checkCephStatus(startupCtx, confPath); err == nil && status.Osdmap.NumUpOsds > 0 {
				return nil
			}
		}
	}
}

type cephStatus struct {
	Monmap cephStatusMonmap `json:"monmap"`
	Osdmap cephStatusOsdmap `json:"osdmap"`
}

type cephStatusMonmap struct {
	NumMons int `json:"num_mons"`
}

type cephStatusOsdmap struct {
	NumUpOsds int `json:"num_up_osds"`
}

func checkCephStatus(ctx context.Context, confPath string) (cephStatus, error) {
	statusCmd := exec.CommandContext(ctx, "ceph", "--conf", confPath, "status", "--format", "json")
	output, err := statusCmd.Output()
	if err != nil {
		return cephStatus{}, err
	}

	var status cephStatus
	err = json.Unmarshal(output, &status)
	if err != nil {
		return cephStatus{}, err
	}

	return status, err
}

func createCephPool(ctx context.Context, confPath string) (string, error) {
	bytes := make([]byte, 4)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	name := "test-" + hex.EncodeToString(bytes)

	cmd := exec.CommandContext(ctx, "ceph", "--conf", confPath, "osd", "pool", "create", name, "1")
	err = cmd.Run()
	if err != nil {
		return "", err
	}

	return name, nil
}

type BufferedLog struct {
	mu        sync.Mutex
	buffer    bytes.Buffer
	streaming bool
	target    io.Writer
}

func (bl *BufferedLog) Write(p []byte) (n int, err error) {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	if bl.streaming {
		return bl.target.Write(p)
	}
	return bl.buffer.Write(p)
}

func (bl *BufferedLog) EnableStream() {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	bl.streaming = true
}

func (bl *BufferedLog) Flush() error {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	if bl.buffer.Len() > 0 {
		_, err := io.Copy(bl.target, &bl.buffer)
		bl.buffer.Reset()
		return err
	}
	return nil
}
