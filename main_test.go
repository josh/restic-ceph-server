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
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
)

var cephDaemonLogs *LogDemux

func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"restic-ceph-server": main,
		"wait4socket":        waitForSocket,
		"rados-object-count": radosObjectCount,
		"scrubhex":           scrubHex,
		"bin-file":           binFile,
		"tail-server-log":    tailServerLog,
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
		_, _ = io.Copy(&TestWriter{t: t}, &setupBuffer)
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
		Setup: func(env *testscript.Env) error {
			poolName, err := createCephPool(ctx, confPath)
			if err != nil {
				return err
			}

			logFile, err := touchServerLog(t, t.TempDir())
			if err != nil {
				return err
			}
			env.Setenv("RESTIC_CEPH_SERVER_LOG_FILE", logFile)

			var testArgs string
			if !deadline.IsZero() {
				env.Setenv("TEST_DEADLINE", deadline.Format(time.RFC3339))
				testArgs = fmt.Sprintf("--verbose --deadline=%s --log-file=%s", deadline.Format(time.RFC3339), logFile)
			} else {
				testArgs = fmt.Sprintf("--verbose --log-file=%s", logFile)
			}
			env.Setenv("TEST_ARGS", testArgs)
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

func tailServerLog() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logFile := os.Getenv("RESTIC_CEPH_SERVER_LOG_FILE")

	f, err := os.Open(logFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open log file for reading: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to close log file: %v\n", err)
			os.Exit(1)
		}
	}()

	reader := bufio.NewReader(f)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				fmt.Fprintf(os.Stderr, "failed to read from log file: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("[restic-ceph-server] %s", line)
		}
	}
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
			"keyring":                               "/dev/null",
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

type TestWriter struct {
	t *testing.T
}

func (tw *TestWriter) Write(p []byte) (n int, err error) {
	tw.t.Helper()
	lines := strings.Split(string(p), "\n")
	for _, line := range lines {
		if line != "" {
			tw.t.Log(line)
		}
	}
	return len(p), nil
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
	w := &TestWriter{t: t}
	ld.outs.Store(w, struct{}{})
	return func() {
		ld.outs.Delete(w)
	}
}

func waitForSocket() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: wait4socket <endpoint> [<endpoint>...]\n")
		os.Exit(1)
	}

	endpoints := os.Args[1:]

	for _, endpoint := range endpoints {
		var success bool

		if strings.Contains(endpoint, ":") {
			for i := 0; i < 30; i++ {
				conn, err := net.DialTimeout("tcp", endpoint, 100*time.Millisecond)
				if err == nil {
					_ = conn.Close()
					success = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !success {
				fmt.Fprintf(os.Stderr, "TCP listener did not respond in time: %s\n", endpoint)
				os.Exit(1)
			}
		} else {
			for i := 0; i < 30; i++ {
				info, err := os.Stat(endpoint)
				if err == nil && info.Mode()&os.ModeSocket != 0 {
					success = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !success {
				fmt.Fprintf(os.Stderr, "socket did not appear in time: %s\n", endpoint)
				os.Exit(1)
			}
		}
	}

	os.Exit(0)
}

func radosObjectCount() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: rados-object-count <prefix>\n")
		os.Exit(1)
	}

	prefix := os.Args[1]
	pool := os.Getenv("CEPH_POOL")
	if pool == "" {
		fmt.Fprintf(os.Stderr, "CEPH_POOL environment variable not set\n")
		os.Exit(1)
	}

	cmd := exec.Command("rados", "--pool", pool, "ls")
	output, err := cmd.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to list rados objects: %v\n", err)
		os.Exit(1)
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
		fmt.Fprintf(os.Stderr, "failed to scan output: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("%d\n", count)
	os.Exit(0)
}

func scrubHex() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: scrubhex <input-file> <output-file>\n")
		fmt.Fprintf(os.Stderr, "  use '-' for stdin/stdout\n")
		os.Exit(1)
	}

	inputPath := os.Args[1]
	outputPath := os.Args[2]

	var input []byte
	var err error
	if inputPath == "-" {
		input, err = io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read stdin: %v\n", err)
			os.Exit(1)
		}
	} else {
		input, err = os.ReadFile(inputPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read input file: %v\n", err)
			os.Exit(1)
		}
	}

	quotedHexPattern := regexp.MustCompile(`"[0-9a-f]{8,}"`)
	output := quotedHexPattern.ReplaceAll(input, []byte(`"[HEX]"`))

	hexPattern := regexp.MustCompile(`[0-9a-f]{8,}`)
	output = hexPattern.ReplaceAll(output, []byte("[HEX]"))

	if outputPath == "-" {
		_, err = os.Stdout.Write(output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to write to stdout: %v\n", err)
			os.Exit(1)
		}
	} else {
		err = os.WriteFile(outputPath, output, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to write output file: %v\n", err)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func binFile() {
	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "usage: bin-file <rand|zeros|ones> <path> <size-bytes>\n")
		os.Exit(1)
	}

	mode := os.Args[1]
	path := os.Args[2]
	sizeStr := os.Args[3]

	if mode != "rand" && mode != "zeros" && mode != "ones" {
		fmt.Fprintf(os.Stderr, "mode must be rand, zeros, or ones\n")
		os.Exit(1)
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid size: %v\n", err)
		os.Exit(1)
	}

	if size < 0 {
		fmt.Fprintf(os.Stderr, "size must be non-negative\n")
		os.Exit(1)
	}

	file, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create file: %v\n", err)
		os.Exit(1)
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
		fmt.Fprintf(os.Stderr, "failed to write file: %v\n", err)
		os.Exit(1)
	}

	os.Exit(0)
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
