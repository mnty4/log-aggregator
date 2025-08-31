package main

import (
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
// aggregate by service, log level each hour
// aggregator shows last hours log outputs for each service and prints to console every 30 sec
//var output []byte
//out := outputValidator{WriteFn: func(p []byte) (n int, err error) {
//output = p
//fmt.Println("output", output)
//return len(p), nil
//}}
// fmt.Println("out", output)
//		outputStrs := strings.Split(string(output), "\n")
//		if diff := cmp.Diff(outputStrs, []string{
//			"2025-01-02 14:00:00-14:59:59 IngestorService: ERROR 0, WARN 0, INFO 1",
//			"2025-01-02 15:00:00-15:59:59 IngestorService: ERROR 0, WARN 1, INFO 1",
//			"2025-01-02 15:00:00-15:59:59 ProcessorService: ERROR 3, WARN 0, INFO 0"}); diff != "" {
//			t.Fatal(diff)
//		}

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
		err := ingest(context.Background(), ErrorIOReader{}, &dal.ProdRedisClient{})
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
		output := []string{}
		mockRedisClient := &dal.MockRedisClient{IncrementFn: func(ctx context.Context, parts []string) error {
			output = append(output, strings.Join(parts, " "))
			return nil
		}}
		if err := ingest(context.Background(), f, mockRedisClient); err != nil {
			t.Fatalf("Expected: nil. Got error: %v", err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var output []byte
	out := outputValidator{WriteFn: func(p []byte) (n int, err error) {
		output = p
		return len(p), nil
	}}
	client := &dal.MockRedisClient{GetFn: func(ctx context.Context, parts []string) (int64, error) {
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
	}}
	if err := aggregate(ctx, out, 10*time.Millisecond, 1*time.Hour, client); err != nil {
		t.Fatalf("Expected nil. Got err: %v", err)
	}
	if diff := cmp.Diff(strings.Split(string(output), "\n"), []string{
		"2025-01-02 14:00:00-14:59:59 IngestorService: ERROR 0, WARN 0, INFO 1",
		"2025-01-02 15:00:00-15:59:59 IngestorService: ERROR 0, WARN 1, INFO 1",
		"2025-01-02 15:00:00-15:59:59 ProcessorService: ERROR 3, WARN 0, INFO 0",
	}); diff != "" {
		t.Fatalf("Expected +. Got -: %s", diff)
	}

}
