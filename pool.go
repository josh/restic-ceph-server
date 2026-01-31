package main

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

type BlobType string

const (
	BlobTypeConfig    BlobType = "config"
	BlobTypeKeys      BlobType = "keys"
	BlobTypeLocks     BlobType = "locks"
	BlobTypeSnapshots BlobType = "snapshots"
	BlobTypeData      BlobType = "data"
	BlobTypeIndex     BlobType = "index"
)

var AllBlobTypes = []BlobType{
	BlobTypeConfig, BlobTypeKeys, BlobTypeLocks,
	BlobTypeSnapshots, BlobTypeData, BlobTypeIndex,
}

type PoolMapping struct {
	typeToPool map[BlobType]string
	pools      []string
}

func (pm *PoolMapping) GetPoolForType(blobType BlobType) string {
	return pm.typeToPool[blobType]
}

func (pm *PoolMapping) Pools() []string {
	return pm.pools
}

func ParsePoolMapping(specs []string) (*PoolMapping, error) {
	if len(specs) == 0 {
		return nil, errors.New("no pool specifications provided")
	}

	typeToPool := make(map[BlobType]string)
	var catchAllPool string
	poolSet := make(map[string]struct{})

	for _, spec := range specs {
		poolName, types, err := parsePoolSpec(spec)
		if err != nil {
			return nil, err
		}

		poolSet[poolName] = struct{}{}

		if len(types) == 0 || (len(types) == 1 && types[0] == "*") {
			if catchAllPool != "" {
				return nil, fmt.Errorf("multiple catch-all pools specified: %q and %q", catchAllPool, poolName)
			}
			catchAllPool = poolName
			continue
		}

		for _, t := range types {
			if t == "*" {
				return nil, fmt.Errorf("pool %q: wildcard '*' cannot be mixed with explicit types", poolName)
			}
			blobType := BlobType(t)
			if !isValidBlobTypeForMapping(blobType) {
				return nil, fmt.Errorf("pool %q: unknown blob type %q", poolName, t)
			}
			if existing, ok := typeToPool[blobType]; ok {
				return nil, fmt.Errorf("blob type %q assigned to multiple pools: %q and %q", t, existing, poolName)
			}
			typeToPool[blobType] = poolName
		}
	}

	for _, bt := range AllBlobTypes {
		if _, ok := typeToPool[bt]; !ok {
			if catchAllPool == "" {
				var missing []string
				for _, bt2 := range AllBlobTypes {
					if _, ok := typeToPool[bt2]; !ok {
						missing = append(missing, string(bt2))
					}
				}
				return nil, fmt.Errorf("blob types not assigned to any pool: %s (use '*' as catch-all)", strings.Join(missing, ", "))
			}
			typeToPool[bt] = catchAllPool
		}
	}

	pools := make([]string, 0, len(poolSet))
	for p := range poolSet {
		pools = append(pools, p)
	}
	slices.Sort(pools)

	return &PoolMapping{
		typeToPool: typeToPool,
		pools:      pools,
	}, nil
}

func parsePoolSpec(spec string) (poolName string, types []string, err error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "", nil, errors.New("empty pool specification")
	}

	colonIdx := strings.Index(spec, ":")
	if colonIdx == -1 {
		return spec, []string{"*"}, nil
	}

	poolName = strings.TrimSpace(spec[:colonIdx])
	if poolName == "" {
		return "", nil, fmt.Errorf("empty pool name in specification: %q", spec)
	}

	typesPart := strings.TrimSpace(spec[colonIdx+1:])
	if typesPart == "" {
		return "", nil, fmt.Errorf("empty types list in specification: %q", spec)
	}

	if typesPart == "*" {
		return poolName, []string{"*"}, nil
	}

	for _, t := range strings.Split(typesPart, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		types = append(types, t)
	}

	if len(types) == 0 {
		return "", nil, fmt.Errorf("no valid types in specification: %q", spec)
	}

	return poolName, types, nil
}

func isValidBlobTypeForMapping(bt BlobType) bool {
	for _, valid := range AllBlobTypes {
		if bt == valid {
			return true
		}
	}
	return false
}

type PoolProperties struct {
	RequiresAlignment bool
	Alignment         uint64
}
