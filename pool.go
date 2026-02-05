package main

import (
	"errors"
	"fmt"
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

type PoolProperties struct {
	RequiresAlignment bool
	Alignment         uint64
}

func ParsePoolsFromCLI(specs []string) (ServerConfigPools, error) {
	if len(specs) == 0 {
		return ServerConfigPools{}, errors.New("no pool specifications provided")
	}

	typeToPool := make(map[BlobType]string)
	var catchAllPool string

	for _, spec := range specs {
		poolName, types, err := parsePoolSpec(spec)
		if err != nil {
			return ServerConfigPools{}, err
		}

		if len(types) == 0 || (len(types) == 1 && types[0] == "*") {
			if catchAllPool != "" {
				return ServerConfigPools{}, fmt.Errorf("multiple catch-all pools specified: %q and %q", catchAllPool, poolName)
			}
			catchAllPool = poolName
			continue
		}

		for _, t := range types {
			if t == "*" {
				return ServerConfigPools{}, fmt.Errorf("pool %q: wildcard '*' cannot be mixed with explicit types", poolName)
			}
			blobType := BlobType(t)
			if !isValidBlobTypeForMapping(blobType) {
				return ServerConfigPools{}, fmt.Errorf("pool %q: unknown blob type %q", poolName, t)
			}
			if existing, ok := typeToPool[blobType]; ok {
				return ServerConfigPools{}, fmt.Errorf("blob type %q assigned to multiple pools: %q and %q", t, existing, poolName)
			}
			typeToPool[blobType] = poolName
		}
	}

	for _, bt := range AllBlobTypes {
		if _, ok := typeToPool[bt]; !ok && catchAllPool != "" {
			typeToPool[bt] = catchAllPool
		}
	}

	return ServerConfigPools{
		Config:    typeToPool[BlobTypeConfig],
		Keys:      typeToPool[BlobTypeKeys],
		Locks:     typeToPool[BlobTypeLocks],
		Snapshots: typeToPool[BlobTypeSnapshots],
		Data:      typeToPool[BlobTypeData],
		Index:     typeToPool[BlobTypeIndex],
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
