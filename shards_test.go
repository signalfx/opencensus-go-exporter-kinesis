package kinesis

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

var xs16 = [][3]string{
	{"0", "21267647932558653966460912964485513215", "shardId-000000000000"},
	{"21267647932558653966460912964485513216", "42535295865117307932921825928971026431", "shardId-000000000001"},
	{"42535295865117307932921825928971026432", "63802943797675961899382738893456539647", "shardId-000000000002"},
	{"63802943797675961899382738893456539648", "85070591730234615865843651857942052863", "shardId-000000000003"},
	{"85070591730234615865843651857942052864", "106338239662793269832304564822427566079", "shardId-000000000004"},
	{"106338239662793269832304564822427566080", "127605887595351923798765477786913079295", "shardId-000000000005"},
	{"127605887595351923798765477786913079296", "148873535527910577765226390751398592511", "shardId-000000000006"},
	{"148873535527910577765226390751398592512", "170141183460469231731687303715884105727", "shardId-000000000007"},
	{"170141183460469231731687303715884105728", "191408831393027885698148216680369618943", "shardId-000000000008"},
	{"191408831393027885698148216680369618944", "212676479325586539664609129644855132159", "shardId-000000000009"},
	{"212676479325586539664609129644855132160", "233944127258145193631070042609340645375", "shardId-000000000010"},
	{"233944127258145193631070042609340645376", "255211775190703847597530955573826158591", "shardId-000000000011"},
	{"255211775190703847597530955573826158592", "276479423123262501563991868538311671807", "shardId-000000000012"},
	{"276479423123262501563991868538311671808", "297747071055821155530452781502797185023", "shardId-000000000013"},
	{"297747071055821155530452781502797185024", "319014718988379809496913694467282698239", "shardId-000000000014"},
	{"319014718988379809496913694467282698240", "340282366920938463463374607431768211455", "shardId-000000000015"},
}

var xs4 = [][3]string{
	{"0", "85070591730234615865843651857942052863", "shardId-000000000000"},
	{"85070591730234615865843651857942052864", "170141183460469231731687303715884105727", "shardId-000000000001"},
	{"170141183460469231731687303715884105728", "255211775190703847597530955573826158591", "shardId-000000000002"},
	{"255211775190703847597530955573826158592", "340282366920938463463374607431768211455", "shardId-000000000003"},
}

const benchTestTraceID = "bd7a977555f6b982bd7a977555f6b982"

func BenchmarkOGShards(b *testing.B) {
	shards := generateV1(xs16)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v, err := getShardProducerIndex(shards, benchTestTraceID)
		if err != nil || v != 13 {
			b.Fatal(err)
		}
	}
}
func BenchmarkNewAShards(b *testing.B) {
	shards := generateV2(xs16)
	var v int
	b.ResetTimer()
	b.ReportAllocs()
	var err error
	for i := 0; i < b.N; i++ {
		v, err = shards.getIndex(benchTestTraceID)
		if v != 13 || err != nil {
			b.Fatal("not 13")
		}
	}
}

func BenchmarkNewAShardsNoPower(b *testing.B) {
	shards := generateV2(xs16[1:])
	var v int
	b.ResetTimer()
	b.ReportAllocs()
	var err error
	for i := 0; i < b.N; i++ {
		v, err = shards.getIndex(benchTestTraceID)
		if v != 12 || err != nil {
			b.Fatal("not 12")
		}
	}
}

func generateV1(xs [][3]string) []*Shard {
	shards := make([]*Shard, len(xs))
	for i, x := range xs {
		s := &Shard{
			shardID:         x[2],
			startingHashKey: toBigInt(x[0]),
			endingHashKey:   toBigInt(x[1]),
		}
		shards[i] = s
	}
	return shards
}

func generateV2(xs [][3]string) *ShardInfo {
	ret := &ShardInfo{}
	for _, x := range xs {
		s := Shard{
			shardID:         x[2],
			startingHashKey: toBigInt(x[0]),
			endingHashKey:   toBigInt(x[1]),
		}
		ret.shards = append(ret.shards, s)
	}
	ret.shiftLen = uint(128 - math.Log2(float64(len(ret.shards))))
	ret.power = math.Ceil(math.Log2(float64(len(ret.shards)))) == math.Floor(math.Log2(float64(len(ret.shards))))

	return ret
}

func getShardProducerIndex(ss []*Shard, traceID string) (int, error) {
	for i, s := range ss {
		key := partitionKeyToHashKey(traceID)
		ok, err := s.belongsToShardKey(key)
		if err != nil {
			return -1, err
		}
		if ok {
			return i, nil
		}
	}
	return -1, fmt.Errorf("no shard found for parition key %s", traceID)
}

func TestEquality(t *testing.T) {
	tests := []struct {
		name string
		test [][3]string
	}{
		{name: "16 vnodes", test: xs16},
		{name: "4 vnodes", test: xs4},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			info := generateV2(test.test)
			shards := generateV1(test.test)
			for _, x := range []string{"bd7a977555f6b982", "0000000000000000bd7a977555f6b982", "bd7a977555f6b982bd7a977555f6b982", "0"} {
				index, _ := getShardProducerIndex(shards, x)
				index2, _ := info.getIndex(x)
				assert.Equal(t, index, index2)
			}
		})
	}
}
