package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/mnty4/LogAggregator/dal"
	"golang.org/x/sync/errgroup"
)

type Clock interface {
	Now() time.Time
}
type MockClock struct {
	NowFn func() time.Time
}
type RealClock struct{}

func (c *MockClock) Now() time.Time {
	return c.NowFn()
}
func (c *RealClock) Now() time.Time {
	return time.Now()
}

type Scanner interface {
	Text() string
	Err() error
	Scan() bool
}

func run(ctx context.Context, logFile string, out io.Writer) error {
	redisClient := dal.NewRedisClient()
	clock := &RealClock{}
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return fsWatch(ctx, logFile, redisClient)
	})
	eg.Go(func() error {
		return aggregate(ctx, out, 10*time.Second, 5*time.Hour, redisClient, clock)
	})
	return eg.Wait()
}

func fsWatch(ctx context.Context, logFile string, redisClient dal.RedisClient) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}
	defer watcher.Close()
	if err = watcher.Add(logFile); err != nil {
		return fmt.Errorf("error adding watcher: %v", err)
	}
	f, err := os.Open(logFile)
	if err != nil {
		return fmt.Errorf("error opening log file: %w", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for {

		if err = ingest(ctx, scanner, redisClient); err != nil {
			fmt.Println(err)
		}
		select {
		case event := <-watcher.Events:
			if event.Has(fsnotify.Write) {
				continue
			}
			if event.Has(fsnotify.Remove | fsnotify.Rename) {
				f.Close()
				f, err = os.Open(logFile)
				if err != nil {
					return fmt.Errorf("error opening log file: %w", err)
				}
				scanner = bufio.NewScanner(f)
				if err = watcher.Add(logFile); err != nil {
					return fmt.Errorf("error adding watcher: %v", err)
				}
			}
		}

	}
}

func ingest(ctx context.Context, scanner Scanner, redisClient dal.RedisClient) error {
	for scanner.Scan() {
		line := scanner.Text()
		if err := processLine(ctx, line, redisClient); err != nil {
			fmt.Printf("Failed to process line: %v\n", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning log file: %w", err)
	}
	return nil
}

func processLine(ctx context.Context, line string, redisClient dal.RedisClient) error {
	parts := strings.Fields(line)
	if len(parts) < 4 {
		return fmt.Errorf("incorrect log format: %s\n", strings.Join(parts, " "))
	}
	date, time_, service, logLevel := parts[0], parts[1], parts[2], parts[3]
	hourMinute := strings.Split(time_, ":")[:2]
	time_ = strings.Join(hourMinute, ":")
	if err := redisClient.Increment(ctx, []string{date, time_, service, logLevel}); err != nil {
		return err
	}
	return nil
}

func aggregate(ctx context.Context, out io.Writer, interval, windowLen time.Duration, redisClient dal.RedisClient, clock Clock) error {
	ticker := time.NewTicker(interval)
	for {
		windowEnd := clock.Now().Round(time.Minute)
		windowStart := windowEnd.Add(-windowLen.Round(time.Minute))
		services := []string{"IngestorService", "ProcessorService"}
		logLevels := []string{"ERROR", "WARN", "INFO"}
		for _, service := range services {
			countByLogLevel := make(map[string]int64)
			for _, logLevel := range logLevels {
				timePoint := windowStart
				for timePoint.Before(windowEnd) {
					date := timePoint.Format(time.DateOnly)
					time_ := timePoint.Format("15:04")
					count, _ := redisClient.Get(ctx, []string{date, time_, service, logLevel})
					countByLogLevel[logLevel] += count
					timePoint = timePoint.Add(time.Minute)
				}
			}
			var lineArr []string
			for _, logLevel := range logLevels {
				lineArr = append(lineArr, fmt.Sprintf("%s %d", logLevel, countByLogLevel[logLevel]))
			}
			if _, err := fmt.Fprintf(out, "%s: %s\n", service, strings.Join(lineArr, ", ")); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func main() {
	var logFile string
	flag.StringVar(&logFile, "logFile", "", "path to log file")
	flag.Parse()

	if logFile == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	if err := run(context.Background(), logFile, os.Stdout); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
