package main

import (
	"testing"
)

func TestParsePoolsFromCLI(t *testing.T) {
	tests := []struct {
		name    string
		specs   []string
		want    ServerConfigPools
		wantErr string
	}{
		{
			name:    "empty specs",
			specs:   []string{},
			wantErr: "no pool specifications provided",
		},
		{
			name:  "single catch-all pool",
			specs: []string{"mypool"},
			want: ServerConfigPools{
				Config:    "mypool",
				Keys:      "mypool",
				Locks:     "mypool",
				Snapshots: "mypool",
				Data:      "mypool",
				Index:     "mypool",
			},
		},
		{
			name:  "explicit catch-all with wildcard",
			specs: []string{"mypool:*"},
			want: ServerConfigPools{
				Config:    "mypool",
				Keys:      "mypool",
				Locks:     "mypool",
				Snapshots: "mypool",
				Data:      "mypool",
				Index:     "mypool",
			},
		},
		{
			name:  "pool with specific types",
			specs: []string{"restic_data:data,index", "restic_metadata:*"},
			want: ServerConfigPools{
				Config:    "restic_metadata",
				Keys:      "restic_metadata",
				Locks:     "restic_metadata",
				Snapshots: "restic_metadata",
				Data:      "restic_data",
				Index:     "restic_data",
			},
		},
		{
			name:  "multiple pools with catch-all for remainder",
			specs: []string{"restic_data:data", "indexpool:index", "restic_metadata:*"},
			want: ServerConfigPools{
				Config:    "restic_metadata",
				Keys:      "restic_metadata",
				Locks:     "restic_metadata",
				Snapshots: "restic_metadata",
				Data:      "restic_data",
				Index:     "indexpool",
			},
		},
		{
			name:  "all types explicitly assigned",
			specs: []string{"pool1:config,keys,locks", "pool2:snapshots,data,index"},
			want: ServerConfigPools{
				Config:    "pool1",
				Keys:      "pool1",
				Locks:     "pool1",
				Snapshots: "pool2",
				Data:      "pool2",
				Index:     "pool2",
			},
		},
		{
			name:    "error: multiple catch-all pools",
			specs:   []string{"pool1", "pool2"},
			wantErr: `multiple catch-all pools specified: "pool1" and "pool2"`,
		},
		{
			name:    "error: multiple catch-all pools with explicit wildcard",
			specs:   []string{"pool1:*", "pool2:*"},
			wantErr: `multiple catch-all pools specified: "pool1" and "pool2"`,
		},
		{
			name:    "error: wildcard mixed with explicit types",
			specs:   []string{"mypool:data,*"},
			wantErr: `pool "mypool": wildcard '*' cannot be mixed with explicit types`,
		},
		{
			name:    "error: unknown blob type",
			specs:   []string{"mypool:data,unknown"},
			wantErr: `pool "mypool": unknown blob type "unknown"`,
		},
		{
			name:    "error: duplicate blob type assignment",
			specs:   []string{"pool1:data", "pool2:data,index", "restic_metadata:*"},
			wantErr: `blob type "data" assigned to multiple pools: "pool1" and "pool2"`,
		},
		{
			name:  "partial mapping without catch-all (allowed)",
			specs: []string{"restic_data:data"},
			want: ServerConfigPools{
				Data: "restic_data",
			},
		},
		{
			name:    "error: empty pool specification",
			specs:   []string{""},
			wantErr: "empty pool specification",
		},
		{
			name:    "error: empty pool name",
			specs:   []string{":data"},
			wantErr: `empty pool name in specification: ":data"`,
		},
		{
			name:    "error: empty types list",
			specs:   []string{"mypool:"},
			wantErr: `empty types list in specification: "mypool:"`,
		},
		{
			name:  "whitespace handling",
			specs: []string{"  restic_data : data , index  ", "restic_metadata:*"},
			want: ServerConfigPools{
				Config:    "restic_metadata",
				Keys:      "restic_metadata",
				Locks:     "restic_metadata",
				Snapshots: "restic_metadata",
				Data:      "restic_data",
				Index:     "restic_data",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePoolsFromCLI(tt.specs)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if err.Error() != tt.wantErr {
					t.Fatalf("expected error %q, got %q", tt.wantErr, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got.Config != tt.want.Config {
				t.Errorf("Config = %q, want %q", got.Config, tt.want.Config)
			}
			if got.Keys != tt.want.Keys {
				t.Errorf("Keys = %q, want %q", got.Keys, tt.want.Keys)
			}
			if got.Locks != tt.want.Locks {
				t.Errorf("Locks = %q, want %q", got.Locks, tt.want.Locks)
			}
			if got.Snapshots != tt.want.Snapshots {
				t.Errorf("Snapshots = %q, want %q", got.Snapshots, tt.want.Snapshots)
			}
			if got.Data != tt.want.Data {
				t.Errorf("Data = %q, want %q", got.Data, tt.want.Data)
			}
			if got.Index != tt.want.Index {
				t.Errorf("Index = %q, want %q", got.Index, tt.want.Index)
			}
		})
	}
}

func TestServerConfigPools_GetPoolForType(t *testing.T) {
	pools := ServerConfigPools{
		Config:    "restic_metadata",
		Keys:      "restic_metadata",
		Locks:     "restic_metadata",
		Snapshots: "restic_metadata",
		Data:      "restic_data",
		Index:     "restic_data",
	}

	tests := []struct {
		blobType BlobType
		want     string
	}{
		{BlobTypeConfig, "restic_metadata"},
		{BlobTypeKeys, "restic_metadata"},
		{BlobTypeLocks, "restic_metadata"},
		{BlobTypeSnapshots, "restic_metadata"},
		{BlobTypeData, "restic_data"},
		{BlobTypeIndex, "restic_data"},
		{BlobType("invalid"), ""},
	}

	for _, tt := range tests {
		t.Run(string(tt.blobType), func(t *testing.T) {
			got := pools.GetPoolForType(tt.blobType)
			if got != tt.want {
				t.Errorf("GetPoolForType(%q) = %q, want %q", tt.blobType, got, tt.want)
			}
		})
	}
}

func TestServerConfigPools_IsComplete(t *testing.T) {
	tests := []struct {
		name         string
		pools        ServerConfigPools
		wantComplete bool
	}{
		{
			name: "complete",
			pools: ServerConfigPools{
				Config:    "pool1",
				Keys:      "pool1",
				Locks:     "pool1",
				Snapshots: "pool1",
				Data:      "pool1",
				Index:     "pool1",
			},
			wantComplete: true,
		},
		{
			name: "incomplete - missing data",
			pools: ServerConfigPools{
				Config:    "pool1",
				Keys:      "pool1",
				Locks:     "pool1",
				Snapshots: "pool1",
				Index:     "pool1",
			},
			wantComplete: false,
		},
		{
			name:         "incomplete - empty",
			pools:        ServerConfigPools{},
			wantComplete: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.pools.IsComplete(); got != tt.wantComplete {
				t.Errorf("IsComplete() = %v, want %v", got, tt.wantComplete)
			}
		})
	}
}

func TestServerConfigPools_UniquePools(t *testing.T) {
	tests := []struct {
		name  string
		pools ServerConfigPools
		want  []string
	}{
		{
			name: "single pool",
			pools: ServerConfigPools{
				Config:    "mypool",
				Keys:      "mypool",
				Locks:     "mypool",
				Snapshots: "mypool",
				Data:      "mypool",
				Index:     "mypool",
			},
			want: []string{"mypool"},
		},
		{
			name: "two pools sorted",
			pools: ServerConfigPools{
				Config:    "apool",
				Keys:      "apool",
				Locks:     "apool",
				Snapshots: "apool",
				Data:      "zpool",
				Index:     "zpool",
			},
			want: []string{"apool", "zpool"},
		},
		{
			name: "multiple pools sorted",
			pools: ServerConfigPools{
				Config:    "restic_metadata",
				Keys:      "restic_metadata",
				Locks:     "restic_metadata",
				Snapshots: "restic_metadata",
				Data:      "restic_data",
				Index:     "indexpool",
			},
			want: []string{"indexpool", "restic_data", "restic_metadata"},
		},
		{
			name: "partial pools",
			pools: ServerConfigPools{
				Config: "configpool",
				Data:   "datapool",
			},
			want: []string{"configpool", "datapool"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.pools.UniquePools()
			if len(got) != len(tt.want) {
				t.Fatalf("UniquePools() returned %d pools, want %d", len(got), len(tt.want))
			}

			for i, want := range tt.want {
				if got[i] != want {
					t.Errorf("UniquePools()[%d] = %q, want %q", i, got[i], want)
				}
			}
		})
	}
}
