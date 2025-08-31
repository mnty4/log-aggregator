package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/mnty4/LogAggregator/dal"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"strings"
	"time"
)

func run(ctx context.Context, logFile string, out io.Writer) error {
	f, err := os.Open(logFile)
	if err != nil {
		return fmt.Errorf("error opening log file: %w", err)
	}
	defer f.Close()
	redisClient := dal.NewRedisClient()
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return ingest(ctx, f, redisClient)
	})
	eg.Go(func() error {
		return aggregate(ctx, out, 10*time.Second, 5*time.Hour, redisClient)
	})
	return eg.Wait()
}
func ingest(ctx context.Context, input io.Reader, redisClient dal.RedisClient) error {
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) < 4 {
			fmt.Fprintf(os.Stderr, "incorrect log format: %s\n", strings.Join(parts, " "))
			continue
		}
		date, time, service, logLevel := parts[0], parts[1], parts[2], parts[3]
		hourMinute := strings.Split(time, ":")[:2]
		time = strings.Join(hourMinute, ":")
		if err := redisClient.Increment(ctx, []string{date, time, service, logLevel}); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning log file: %w", err)
	}
	return nil
}
func aggregate(ctx context.Context, out io.Writer, interval, windowLen time.Duration, redisClient dal.RedisClient) error {
	ticker := time.NewTicker(interval)
	for {
		windowEnd := time.Now().Round(time.Minute)
		windowStart := windowEnd.Add(-windowLen.Round(time.Minute))
		services := []string{"IngestorService", "ProcessorService"}
		logLevels := []string{"ERROR", "WARN", "INFO"}
		for _, service := range services {
			countByLogLevel := make(map[string]int64)
			for _, logLevel := range logLevels {
				timePoint := windowStart
				for timePoint.Before(windowEnd) {
					date := timePoint.Format(time.DateOnly)
					time_ := timePoint.Format(time.TimeOnly)
					count, _ := redisClient.Get(ctx, []string{date, time_, service, logLevel})
					countByLogLevel[logLevel] += count
					timePoint.Add(-time.Minute)
				}
			}
			var lineArr []string
			for _, logLevel := range logLevels {
				lineArr = append(lineArr, fmt.Sprintf("%s: %d", logLevel, countByLogLevel[logLevel]))
			}
			if _, err := fmt.Fprintf(out, "%s %s", service, strings.Join(lineArr, ", ")); err != nil {
				return err
			}
		}
		<-ticker.C
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
