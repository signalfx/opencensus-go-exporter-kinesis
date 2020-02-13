package kinesis

import (
	"crypto/md5"
	"fmt"
	"math"
	"math/big"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Shard holds the information for a kinesis shard
type Shard struct {
	shardID         string
	startingHashKey *big.Int
	endingHashKey   *big.Int
}

// ShardInfo provides a way to find the index for which shard to put a span in
type ShardInfo struct {
	shiftLen uint    // number of bits to shift to get to an int
	shards   []Shard // use for names and backup if not a power of 2
	power    bool    // to know if its a power of 2
}

func (s *ShardInfo) getIndex(traceID string) (int, error) {
	if len(s.shards) == 1 {
		return 0, nil
	}
	key := partitionKeyToHashKey(traceID)
	if s.power {
		rshift := (&big.Int{}).Rsh(key, s.shiftLen)
		return int(rshift.Int64()), nil
	}
	// honestly this should be a tree, but it would have to be a custom one so probably not worth the effort since nearly everyone is a power of 2
	for i, s := range s.shards {
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

func getShardInfo(k *kinesis.Kinesis, streamName string) (*ShardInfo, error) {
	listShardsInput := &kinesis.ListShardsInput{
		StreamName: aws.String(streamName),
		MaxResults: aws.Int64(100),
	}
	ret := &ShardInfo{}

	for {
		resp, err := k.ListShards(listShardsInput)
		if err != nil {
			return nil, fmt.Errorf("listShards error: %v", err)
		}

		for _, s := range resp.Shards {
			// shard is closed so skip it
			if s.SequenceNumberRange.EndingSequenceNumber != nil {
				continue
			}
			s := Shard{
				shardID:         *s.ShardId,
				startingHashKey: toBigInt(*s.HashKeyRange.StartingHashKey),
				endingHashKey:   toBigInt(*s.HashKeyRange.EndingHashKey),
			}
			ret.shards = append(ret.shards, s)
		}

		if resp.NextToken == nil {
			ret.power = math.Ceil(math.Log2(float64(len(ret.shards)))) == math.Floor(math.Log2(float64(len(ret.shards))))
			ret.shiftLen = uint(128 - math.Log2(float64(len(ret.shards))))
			return ret, nil
		}

		listShardsInput = &kinesis.ListShardsInput{
			NextToken: resp.NextToken,
		}
	}
}

func toBigInt(key string) *big.Int {
	num := big.NewInt(0)
	num.SetString(key, 10)
	return num
}

func (s *Shard) belongsToShardKey(key *big.Int) (bool, error) {
	return key.Cmp(s.startingHashKey) >= 0 && key.Cmp(s.endingHashKey) <= 0, nil
}

func partitionKeyToHashKey(partitionKey string) *big.Int {
	b := md5.Sum([]byte(partitionKey))
	return big.NewInt(0).SetBytes(b[:])
}
