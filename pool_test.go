package main

import (
	"testing"
)

func TestParsePoolMapping(t *testing.T) {
	tests := []struct {
		name    string
		specs   []string
		want    map[BlobType]string
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
			want: map[BlobType]string{
				BlobTypeConfig:    "mypool",
				BlobTypeKeys:      "mypool",
				BlobTypeLocks:     "mypool",
				BlobTypeSnapshots: "mypool",
				BlobTypeData:      "mypool",
				BlobTypeIndex:     "mypool",
			},
		},
		{
			name:  "explicit catch-all with wildcard",
			specs: []string{"mypool:*"},
			want: map[BlobType]string{
				BlobTypeConfig:    "mypool",
				BlobTypeKeys:      "mypool",
				BlobTypeLocks:     "mypool",
				BlobTypeSnapshots: "mypool",
				BlobTypeData:      "mypool",
				BlobTypeIndex:     "mypool",
			},
		},
		{
			name:  "pool with specific types",
			specs: []string{"restic_data:data,index", "restic_metadata:*"},
			want: map[BlobType]string{
				BlobTypeConfig:    "restic_metadata",
				BlobTypeKeys:      "restic_metadata",
				BlobTypeLocks:     "restic_metadata",
				BlobTypeSnapshots: "restic_metadata",
				BlobTypeData:      "restic_data",
				BlobTypeIndex:     "restic_data",
			},
		},
		{
			name:  "multiple pools with catch-all for remainder",
			specs: []string{"restic_data:data", "indexpool:index", "restic_metadata:*"},
			want: map[BlobType]string{
				BlobTypeConfig:    "restic_metadata",
				BlobTypeKeys:      "restic_metadata",
				BlobTypeLocks:     "restic_metadata",
				BlobTypeSnapshots: "restic_metadata",
				BlobTypeData:      "restic_data",
				BlobTypeIndex:     "indexpool",
			},
		},
		{
			name:  "all types explicitly assigned",
			specs: []string{"pool1:config,keys,locks", "pool2:snapshots,data,index"},
			want: map[BlobType]string{
				BlobTypeConfig:    "pool1",
				BlobTypeKeys:      "pool1",
				BlobTypeLocks:     "pool1",
				BlobTypeSnapshots: "pool2",
				BlobTypeData:      "pool2",
				BlobTypeIndex:     "pool2",
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
			name:    "error: missing blob types without catch-all",
			specs:   []string{"restic_data:data"},
			wantErr: "blob types not assigned to any pool: config, keys, locks, snapshots, index (use '*' as catch-all)",
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
			want: map[BlobType]string{
				BlobTypeConfig:    "restic_metadata",
				BlobTypeKeys:      "restic_metadata",
				BlobTypeLocks:     "restic_metadata",
				BlobTypeSnapshots: "restic_metadata",
				BlobTypeData:      "restic_data",
				BlobTypeIndex:     "restic_data",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePoolMapping(tt.specs)

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

			if got == nil {
				t.Fatal("expected non-nil PoolMapping")
			}

			for blobType, wantPool := range tt.want {
				gotPool := got.GetPoolForType(blobType)
				if gotPool != wantPool {
					t.Errorf("GetPoolForType(%q) = %q, want %q", blobType, gotPool, wantPool)
				}
			}

			for _, blobType := range AllBlobTypes {
				if _, ok := tt.want[blobType]; !ok {
					t.Errorf("test case missing expected mapping for blob type %q", blobType)
				}
			}
		})
	}
}

func TestPoolMapping_GetPoolForType(t *testing.T) {
	pm, err := ParsePoolMapping([]string{"restic_data:data,index", "restic_metadata:*"})
	if err != nil {
		t.Fatalf("ParsePoolMapping failed: %v", err)
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
	}

	for _, tt := range tests {
		t.Run(string(tt.blobType), func(t *testing.T) {
			got := pm.GetPoolForType(tt.blobType)
			if got != tt.want {
				t.Errorf("GetPoolForType(%q) = %q, want %q", tt.blobType, got, tt.want)
			}
		})
	}
}

func TestPoolMapping_Pools(t *testing.T) {
	tests := []struct {
		name  string
		specs []string
		want  []string
	}{
		{
			name:  "single pool",
			specs: []string{"mypool"},
			want:  []string{"mypool"},
		},
		{
			name:  "two pools sorted",
			specs: []string{"zpool:data", "apool:*"},
			want:  []string{"apool", "zpool"},
		},
		{
			name:  "multiple pools sorted",
			specs: []string{"restic_data:data", "indexpool:index", "restic_metadata:*"},
			want:  []string{"indexpool", "restic_data", "restic_metadata"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, err := ParsePoolMapping(tt.specs)
			if err != nil {
				t.Fatalf("ParsePoolMapping failed: %v", err)
			}

			got := pm.Pools()
			if len(got) != len(tt.want) {
				t.Fatalf("Pools() returned %d pools, want %d", len(got), len(tt.want))
			}

			for i, want := range tt.want {
				if got[i] != want {
					t.Errorf("Pools()[%d] = %q, want %q", i, got[i], want)
				}
			}
		})
	}
}
