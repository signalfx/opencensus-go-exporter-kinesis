package kinesis

import (
	"crypto/md5"
	"fmt"
	"math/big"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Shard struct {
	shardId         string
	startingHashKey *big.Int
	endingHashKey   *big.Int
}

func getShards(k *kinesis.Kinesis, streamName string) ([]*Shard, error) {

	listShardsInput := &kinesis.ListShardsInput{
		StreamName: aws.String(streamName),
		MaxResults: aws.Int64(100),
	}

	shards := []*Shard{}
	for {
		resp, err := k.ListShards(listShardsInput)
		if err != nil {
			return nil, fmt.Errorf("ListShards error: %v", err)
		}

		for _, s := range resp.Shards {
			// shard is closed so skip it
			if s.SequenceNumberRange.EndingSequenceNumber != nil {
				continue
			}
			shards = append(shards, &Shard{
				shardId:         *s.ShardId,
				startingHashKey: toBigInt(*s.HashKeyRange.StartingHashKey),
				endingHashKey:   toBigInt(*s.HashKeyRange.EndingHashKey),
			})
		}

		if resp.NextToken == nil {
			return shards, nil
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

func (s *Shard) belongsToShard(partitionKey string) (bool, error) {
	key := s.partitionKeyToHashKey(partitionKey)
	return key.Cmp(s.startingHashKey) >= 0 && key.Cmp(s.endingHashKey) <= 0, nil
}

func (s *Shard) partitionKeyToHashKey(partitionKey string) *big.Int {
	b := md5.Sum([]byte(partitionKey))
	return big.NewInt(0).SetBytes(b[:])
}
