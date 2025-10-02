package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
)

func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"restic-ceph-server": main,
	})
}

func TestScript(t *testing.T) {
	ctx := t.Context()

	confPath, err := setupCephDir(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	err = startCephMon(ctx, confPath)
	if err != nil {
		t.Fatal(err)
	}

	err = startCephOsd(ctx, confPath)
	if err != nil {
		t.Fatal(err)
	}

	updateScripts, _ := strconv.ParseBool(os.Getenv("UPDATE_SCRIPTS"))

	testscript.Run(t, testscript.Params{
		Dir:             "testdata",
		ContinueOnError: true,
		UpdateScripts:   updateScripts,
		Setup: func(env *testscript.Env) error {
			poolName, err := createCephPool(ctx, confPath)
			if err != nil {
				return err
			}

			if deadline, ok := ctx.Deadline(); ok {
				env.Setenv("RESTIC_CEPH_SERVER_DEADLINE", deadline.Format(time.RFC3339))
			}
			env.Setenv("CEPH_CONF", confPath)
			env.Setenv("CEPH_POOL", poolName)

			return nil
		},
	})
}

func setupCephDir(ctx context.Context, tmpDir string) (string, error) {
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
			"keyring":                               filepath.Join(tmpDir, "keyring"),
			"log_to_file":                           "false",
			"log_to_stderr":                         "true",
		},
		"mon": {
			"mon_initial_members":       "mon1",
			"mon_data":                  filepath.Join(tmpDir, "mon", "ceph-$id"),
			"mon_cluster_log_to_file":   "false",
			"mon_cluster_log_to_stderr": "true",
		},
		"osd": {
			"osd_data":        filepath.Join(tmpDir, "osd", "ceph-$id"),
			"osd_objectstore": "memstore",
		},
	}

	keyringConfig := map[string]map[string]string{
		"mon.": {
			"key":      "AQBDm89oNP7bAxAA6TgZ1toOkhDjUNEkRL18Gg==",
			"caps mon": "allow *",
		},
		"client.admin": {
			"key":      "AQB5m89objcKIxAAda2ULz/l3NH+mv9XzKePHQ==",
			"caps mon": "allow *",
			"caps msd": "allow *",
			"caps osd": "allow *",
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

	keyringContent := generateINIConfig(keyringConfig)
	err = os.WriteFile(filepath.Join(tmpDir, "keyring"), []byte(keyringContent), 0o644)
	if err != nil {
		return confPath, err
	}

	monmapPath := filepath.Join(tmpDir, "monmap")
	cmd := exec.CommandContext(ctx, "monmaptool", "--conf", confPath, monmapPath, "--create", "--fsid", fsid)
	if err := cmd.Run(); err != nil {
		return confPath, fmt.Errorf("failed to create monitor map: %w", err)
	}

	cmd = exec.CommandContext(ctx, "monmaptool", "--conf", confPath, monmapPath, "--add", "mon1", "127.0.0.1:6789")
	if err := cmd.Run(); err != nil {
		return confPath, fmt.Errorf("failed to add monitor to map: %w", err)
	}

	cmd = exec.CommandContext(ctx, "ceph-mon", "--conf", confPath, "--mkfs", "--id", "mon1", "--monmap", monmapPath)
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

func startCephMon(ctx context.Context, confPath string) error {
	cmd := exec.CommandContext(ctx, "ceph-mon", "--conf", confPath, "--id", "mon1", "--foreground")

	if testing.Verbose() {
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
	}

	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to spawn ceph-mon: %w", err)
	}

	for range 10 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if status, err := checkCephStatus(ctx, confPath); err == nil && status.Monmap.NumMons > 0 {
			return nil
		}
		time.Sleep(1 * time.Second)
		continue
	}

	return fmt.Errorf("mon.mon1 timed out before becoming ready")
}

func startCephOsd(ctx context.Context, confPath string) error {
	cmd := exec.CommandContext(ctx, "ceph-osd", "--conf", confPath, "--id", "0", "--mkfs")
	if testing.Verbose() {
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
	}

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to initialize OSD filesystem: %w", err)
	}

	cmd = exec.CommandContext(ctx, "ceph-osd", "--conf", confPath, "--id", "0", "--foreground")
	if testing.Verbose() {
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
	}

	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start OSD: %w", err)
	}

	for range 10 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if status, err := checkCephStatus(ctx, confPath); err == nil {
			if status.Osdmap.NumUpOsds > 0 {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
		continue
	}

	cmd.Process.Signal(syscall.SIGTERM)
	cmd.Wait()
	return fmt.Errorf("osd.0 timed out before becoming ready")
}

type cephStatus struct {
	Fsid   string           `json:"fsid"`
	Health cephHealth       `json:"health"`
	Monmap cephStatusMonmap `json:"monmap"`
	Osdmap cephStatusOsdmap `json:"osdmap"`
}

type cephHealth struct {
	Status string `json:"status"`
}

type cephStatusMonmap struct {
	Epoch   int `json:"epoch"`
	NumMons int `json:"num_mons"`
}

type cephStatusOsdmap struct {
	Epoch     int `json:"epoch"`
	NumOsds   int `json:"num_osds"`
	NumInOsds int `json:"num_in_osds"`
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
	cmd := exec.CommandContext(ctx, "ceph", "--conf", confPath, "osd", "pool", "create", name, "8")
	err = cmd.Run()
	if err != nil {
		return "", err
	}

	return name, nil
}
