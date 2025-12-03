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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
)

var cephDaemonLogs *LogDemux

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

	cephDaemonLogs = &LogDemux{}

	var setupBuffer bytes.Buffer
	detachSetup := cephDaemonLogs.Attach(&setupBuffer)
	confPath, err := startCephCluster(t, ctx, cephDaemonLogs)
	if err != nil {
		detachSetup()
		t.Log("\n=== Ceph cluster setup logs ===")
		_, _ = io.Copy(t.Output(), &setupBuffer)
		t.Fatal(err)
	}
	detachSetup()

	detach := cephDaemonLogs.AttachTest(t)
	defer detach()

	updateScripts, _ := strconv.ParseBool(os.Getenv("UPDATE_SCRIPTS"))

	testscript.Run(t, testscript.Params{
		Dir:                 "testdata",
		ContinueOnError:     true,
		RequireExplicitExec: true,
		UpdateScripts:       updateScripts,
		Deadline:            deadline,
		Cmds: map[string]func(*testscript.TestScript, bool, []string){
			"tail-server-log":    cmdTailServerLog,
			"wait4socket":        cmdWait4socket,
			"rados-object-count": cmdRadosObjectCount,
			"scrubhex":           cmdScrubHex,
			"bin-file":           cmdBinFile,
		},
		Setup: func(env *testscript.Env) error {
			scriptCtx, cancel := context.WithCancel(ctx)
			env.Defer(cancel)
			env.Values["ctx"] = scriptCtx

			poolName, err := createCephPool(ctx, confPath)
			if err != nil {
				return err
			}

			logFile, err := touchServerLog(t, t.TempDir())
			if err != nil {
				return err
			}
			env.Setenv("CEPH_SERVER_VERBOSE", "true")
			env.Setenv("CEPH_SERVER_LOG_FILE", logFile)

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

func cmdTailServerLog(ts *testscript.TestScript, neg bool, args []string) {
	ctx, ok := ts.Value("ctx").(context.Context)
	if !ok {
		ts.Fatalf("context not found in testscript Env.Values")
	}

	if neg {
		ts.Fatalf("unsupported: ! tail-server-log")
	}

	logFile := ts.Getenv("CEPH_SERVER_LOG_FILE")
	if logFile == "" {
		ts.Fatalf("CEPH_SERVER_LOG_FILE not set")
	}

	f, err := os.Open(logFile)
	if err != nil {
		ts.Fatalf("failed to open log file for reading: %v", err)
	}
	ts.Defer(func() {
		if err := f.Close(); err != nil {
			ts.Logf("failed to close log file: %v", err)
		}
	})

	go func() {
		reader := bufio.NewReader(f)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				line, err := reader.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						continue
					}
					ts.Logf("tail-server-log: failed to read from log file: %v", err)
					return
				}
				ts.Logf("[restic-ceph-server] %s", strings.TrimRight(line, "\n"))
			}
		}
	}()
}

func startCephCluster(t *testing.T, ctx context.Context, out io.Writer) (string, error) {
	t.Helper()

	startupCtx, startupCancel := context.WithTimeout(ctx, 10*time.Second)
	defer startupCancel()

	tmpDir := t.TempDir()
	confPath, err := setupCephDir(startupCtx, tmpDir, out)
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
			"crash_dir":                             filepath.Join(tmpDir, "crash"),
			"exporter_sock_dir":                     filepath.Join(tmpDir, "run"),
			"immutable_object_cache_sock":           filepath.Join(tmpDir, "run", "immutable_object_cache.sock"),
			"keyring":                               "/dev/null",
			"run_dir":                               filepath.Join(tmpDir, "run"),
			"log_to_file":                           "false",
			"log_to_stderr":                         "true",
			"osd_max_object_size":                   "16777216", // 16Mi
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

	err = os.MkdirAll(filepath.Join(tmpDir, "run"), 0o755)
	if err != nil {
		return confPath, err
	}

	err = os.MkdirAll(filepath.Join(tmpDir, "crash"), 0o755)
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

type LogDemux struct {
	outs sync.Map
}

func (ld *LogDemux) Write(p []byte) (n int, err error) {
	var writeErr error
	ld.outs.Range(func(key, _ interface{}) bool {
		if writer, ok := key.(io.Writer); ok {
			if written, err := writer.Write(p); err != nil {
				writeErr = err
				return false
			} else if written != len(p) {
				writeErr = fmt.Errorf("short write: expected %d, got %d", len(p), written)
				return false
			}
		}
		return true
	})

	if writeErr != nil {
		return 0, writeErr
	}
	return len(p), nil
}

func (ld *LogDemux) Attach(writer io.Writer) func() {
	ld.outs.Store(writer, struct{}{})
	return func() {
		ld.outs.Delete(writer)
	}
}

func (ld *LogDemux) AttachTest(t *testing.T) func() {
	t.Helper()
	return ld.Attach(t.Output())
}

func cmdWait4socket(ts *testscript.TestScript, neg bool, args []string) {
	ctx, ok := ts.Value("ctx").(context.Context)
	if !ok {
		ts.Fatalf("context not found in testscript Env.Values")
	}

	if neg {
		ts.Fatalf("unsupported: ! wait4socket")
	}
	if len(args) < 1 {
		ts.Fatalf("usage: wait4socket <endpoint> [<endpoint>...]")
	}

	for _, endpoint := range args {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		timeout := time.After(3 * time.Second)
		var success bool

		if strings.Contains(endpoint, ":") {
			for !success {
				select {
				case <-ctx.Done():
					ts.Fatalf("context cancelled while waiting for %s: %v", endpoint, ctx.Err())
				case <-timeout:
					ts.Fatalf("TCP listener did not respond in time: %s", endpoint)
				case <-ticker.C:
					conn, err := net.DialTimeout("tcp", endpoint, 100*time.Millisecond)
					if err == nil {
						_ = conn.Close()
						success = true
					}
				}
			}
		} else {
			for !success {
				select {
				case <-ctx.Done():
					ts.Fatalf("context cancelled while waiting for %s: %v", endpoint, ctx.Err())
				case <-timeout:
					ts.Fatalf("socket did not appear in time: %s", endpoint)
				case <-ticker.C:
					info, err := os.Stat(endpoint)
					if err == nil && info.Mode()&os.ModeSocket != 0 {
						success = true
					}
				}
			}
		}
	}
}

func cmdRadosObjectCount(ts *testscript.TestScript, neg bool, args []string) {
	ctx, ok := ts.Value("ctx").(context.Context)
	if !ok {
		ts.Fatalf("context not found in testscript Env.Values")
	}

	if neg {
		ts.Fatalf("unsupported: ! rados-object-count")
	}
	if len(args) != 1 {
		ts.Fatalf("usage: rados-object-count <prefix>")
	}

	prefix := args[0]
	pool := ts.Getenv("CEPH_POOL")
	if pool == "" {
		ts.Fatalf("CEPH_POOL environment variable not set")
	}

	confPath := ts.Getenv("CEPH_CONF")
	if confPath == "" {
		ts.Fatalf("CEPH_CONF environment variable not set")
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "rados", "--conf", confPath, "--pool", pool, "ls")
	output, err := cmd.CombinedOutput()
	if err != nil {
		ts.Fatalf("failed to list rados objects: %v\noutput: %s", err, string(output))
	}

	count := 0
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, prefix) {
			count++
		}
	}

	if err := scanner.Err(); err != nil {
		ts.Fatalf("failed to scan output: %v", err)
	}

	_, _ = fmt.Fprintf(ts.Stdout(), "%d\n", count)
}

func cmdScrubHex(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! scrubhex")
	}
	if len(args) != 2 {
		ts.Fatalf("usage: scrubhex <input-file> <output-file>\n  use 'stdin' for reading previous command output, 'stdout' for writing to stdout")
	}

	inputPath := args[0]
	outputPath := args[1]

	var input []byte
	var err error

	switch inputPath {
	case "stdin":
		input = []byte(ts.ReadFile("stdin"))
	case "stdout":
		input = []byte(ts.ReadFile("stdout"))
	case "stderr":
		input = []byte(ts.ReadFile("stderr"))
	default:
		input, err = os.ReadFile(ts.MkAbs(inputPath))
		if err != nil {
			ts.Fatalf("failed to read input file: %v", err)
		}
	}

	quotedHexPattern := regexp.MustCompile(`"[0-9a-f]{8,}"`)
	output := quotedHexPattern.ReplaceAll(input, []byte(`"[HEX]"`))

	hexPattern := regexp.MustCompile(`[0-9a-f]{8,}`)
	output = hexPattern.ReplaceAll(output, []byte("[HEX]"))

	switch outputPath {
	case "stdout":
		_, _ = ts.Stdout().Write(output)
	case "stderr":
		_, _ = ts.Stderr().Write(output)
	default:
		err = os.WriteFile(ts.MkAbs(outputPath), output, 0o644)
		if err != nil {
			ts.Fatalf("failed to write output file: %v", err)
		}
	}
}

func cmdBinFile(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! bin-file")
	}
	if len(args) != 3 {
		ts.Fatalf("usage: bin-file <rand|zeros|ones> <path> <size-bytes>")
	}

	mode := args[0]
	path := args[1]
	sizeStr := args[2]

	if mode != "rand" && mode != "zeros" && mode != "ones" {
		ts.Fatalf("mode must be rand, zeros, or ones")
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		ts.Fatalf("invalid size: %v", err)
	}

	if size < 0 {
		ts.Fatalf("size must be non-negative")
	}

	file, err := os.Create(ts.MkAbs(path))
	if err != nil {
		ts.Fatalf("failed to create file: %v", err)
	}
	defer func() { _ = file.Close() }()

	var reader io.Reader
	switch mode {
	case "rand":
		reader = io.LimitReader(rand.Reader, size)
	case "zeros":
		reader = io.LimitReader(zeroReader{}, size)
	case "ones":
		reader = io.LimitReader(onesReader{}, size)
	}

	_, err = io.Copy(file, reader)
	if err != nil {
		ts.Fatalf("failed to write file: %v", err)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

type onesReader struct{}

func (onesReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0xFF
	}
	return len(p), nil
}
