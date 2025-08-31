package dal

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/redis/go-redis/v9"
)

func Test_Increment(t *testing.T) {
	output := []string{}
	client := ProdRedisClient{client: &mockInternalRedisClient{IncrFn: func(ctx context.Context, key string) *redis.IntCmd {
		output = append(output, key)
		return redis.NewIntCmd(ctx, 0, nil)
	}}}
	if err := client.Increment(context.Background(), []string{"part1 part2 part3"}); err != nil {
		t.Fatalf("Expected: nil. Got err: %v", err)
	}
	if err := client.Increment(context.Background(), []string{"testpart"}); err != nil {
		t.Fatalf("Expected: nil. Got err: %v", err)
	}
	if diff := cmp.Diff(output, []string{
		"part1 part2 part3",
		"testpart",
	}); diff != "" {
		t.Fatalf("Expected +. Got -: %s", diff)
	}
}

//func Test_
