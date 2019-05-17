package kinesis

import (
	"fmt"
	"math/big"
	"math/rand"
	"strconv"
	"testing"

	"github.com/huichen/murmur"
	gen "github.com/jaegertracing/jaeger/model"
)

// 100 kinesis shards
// const shardSpace string = "3402823669209384634633746074317682114"
// const totalShards int = 10

// 10 kinesis shards
const shardSpace string = "34028236692093848235284053891034906624"
const totalShards int = 10

var shards = []Shard{}

// for murmur hashes that fall in <20 distribution,
// make sure all those 20% cases have even kinesis distributiom

func setupShards(t *testing.T) {
	size := &big.Int{}
	size.SetString(shardSpace, 10)
	lower := &big.Int{}
	lower.SetInt64(0)

	for i := 0; i < totalShards; i++ {
		upper := &big.Int{}
		upper.SetInt64(0)
		upper.Add(lower, size)
		shards = append(shards, Shard{
			shardId:         strconv.Itoa(i),
			startingHashKey: lower,
			endingHashKey:   upper,
		})
		lower = upper
	}

	t.Log("100 shards distributin: ")
	for _, s := range shards {
		t.Logf("%s: %s - %s", s.shardId, s.startingHashKey.String(), s.endingHashKey.String())
	}
}

func TestHash(t *testing.T) {
	setupShards(t)

	distribution := map[string]int{}
	for i := 0; i < 10; i++ {
		distribution[strconv.Itoa(i)] = 0
	}

	for i := 0; i <= 1000000; i++ {
		traceID := gen.NewTraceID(rand.Uint64(), rand.Uint64()).String()
		bucket := murmurHash(traceID)
		if bucket <= 20 {
			shard, err := getShard(traceID)
			if err != nil {
				t.Fatal(err.Error())
			}
			distribution[shard.shardId] = distribution[shard.shardId] + 1
		}
	}
	t.Log("== kinesis distribution: ")
	for k, v := range distribution {
		t.Logf("%s: %d", k, v)
	}
}

func murmurHash(s string) uint32 {
	x := murmur.Murmur3([]byte(s))
	return x % 100
}

func getShard(partitionKey string) (*Shard, error) {
	for _, s := range shards {
		ok, err := s.belongsToShard(partitionKey)
		if err != nil {
			return nil, err
		}
		if ok {
			return &s, nil
		}
	}
	return nil, fmt.Errorf("no shard found for parition key %s", partitionKey)
}
