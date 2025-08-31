package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/mnty4/LogAggregator/dal"

	"os"
	"strings"
	"testing"
)

// TODO
// concurrent log file readers
// aggregator shows last hours log outputs for each service and prints to console every 30 sec
// ingest continuously
// if reaches the end of a file
// handle empty lines

type ErrorIOReader struct {
}

func (ErrorIOReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("test IOReader error")
}

type outputValidator struct {
	WriteFn func(p []byte) (n int, err error)
}

func (o outputValidator) Write(p []byte) (n int, err error) {
	return o.WriteFn(p)
}

func Test_Run(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		os.Remove("missing.log")
		if err := run(context.Background(), "missing.log", os.Stdout); err == nil {
			t.Fatal("Expected: error. Got nil")
		}
	})
}
func Test_Ingest(t *testing.T) {
	t.Run("scanner error", func(t *testing.T) {
		scanner := bufio.NewScanner(ErrorIOReader{})
		err := ingest(context.Background(), scanner, &dal.ProdRedisClient{})
		if err == nil {
			t.Fatal("Expected: scanner err. Got: nil")
		}
		if !strings.Contains(err.Error(), "test IOReader error") {
			t.Fatalf("Expected to contain: test IOReader error. Got: %s", err.Error())
		}
	})
	t.Run("successfully writes to Redis", func(t *testing.T) {
		f, err := os.Open("example.log")
		if err != nil {
			t.Fatal("error opening example.log")
		}
		scanner := bufio.NewScanner(f)
		output := []string{}
		mockRedisClient := &dal.MockRedisClient{IncrementFn: func(ctx context.Context, parts []string) error {
			output = append(output, strings.Join(parts, " "))
			return nil
		}}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		errCh := make(chan error, 1)
		select {
		case errCh <- ingest(context.Background(), scanner, mockRedisClient):
			t.Fatalf("Expected to reach ctx deadline. Got: %v", <-errCh)
		case <-ctx.Done():
		}
		if diff := cmp.Diff(output, []string{
			"2025-01-02 15:03 IngestorService WARN",
			"2025-01-02 15:04 ProcessorService ERROR",
			"2025-01-02 15:05 ProcessorService ERROR",
			"2025-01-02 14:04 IngestorService INFO",
			"2025-01-02 15:04 IngestorService INFO",
			"2025-01-02 15:05 ProcessorService ERROR",
		}); diff != "" {
			t.Fatal(diff)
		}
	})
}

func Test_Aggregate(t *testing.T) {
	testCases := []struct {
		name      string
		interval  time.Duration
		windowLen time.Duration
		client    dal.RedisClient
		clock     Clock
		expected  []string
	}{
		{
			name:      "old log count in redis",
			interval:  time.Hour,
			windowLen: time.Hour,
			client: &dal.MockRedisClient{GetFn: func(ctx context.Context, parts []string) (int64, error) {
				switch strings.Join(parts, " ") {
				case "2025-01-02 14:05 IngestorService ERROR":
					return 1, nil
				case "2025-01-02 15:45 IngestorService ERROR":
					return 3, nil
				case "2025-01-02 15:15 IngestorService WARN":
					return 1, nil
				case "2025-01-02 15:15 ProcessorService INFO":
					return 3, nil
				default:
					return 0, errors.New("missing key")
				}
			}},
			clock: &MockClock{NowFn: func() time.Time {
				time_, _ := time.Parse(time.DateTime, "2025-01-02 15:50:05")
				return time_
			}},
			expected: []string{
				"IngestorService: ERROR 3, WARN 1, INFO 0",
				"ProcessorService: ERROR 0, WARN 0, INFO 3",
			},
		},
		{
			name:      "empty logs",
			interval:  time.Hour,
			windowLen: time.Hour,
			client: &dal.MockRedisClient{GetFn: func(ctx context.Context, parts []string) (int64, error) {
				switch strings.Join(parts, " ") {
				default:
					return 0, errors.New("missing key")
				}
			}},
			clock: &MockClock{NowFn: func() time.Time {
				time_, _ := time.Parse(time.DateTime, "2025-01-02 15:50:05")
				return time_
			}},
			expected: []string{
				"IngestorService: ERROR 0, WARN 0, INFO 0",
				"ProcessorService: ERROR 0, WARN 0, INFO 0",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var output []byte
			out := outputValidator{WriteFn: func(p []byte) (n int, err error) {
				output = append(output, p...)
				return len(p), nil
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			if err := aggregate(ctx, out, 1*time.Hour, 1*time.Hour, testCase.client, testCase.clock); err != nil {
				t.Fatalf("Expected nil. Got err: %v", err)
			}
			if diff := cmp.Diff(strings.Split(strings.TrimSpace(string(output)), "\n"), testCase.expected); diff != "" {
				t.Fatalf("Expected +. Got -: %s", diff)
			}
		})
	}

}
