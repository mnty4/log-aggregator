# Log Aggregator
**Input**: a stream of log lines from multiple services (could be JSON with timestamp, service, level, msg).  
**Task**: build a CLI tool that consumes logs, groups them by service and level, and outputs hourly counts.  
**Stretch**: add concurrency so multiple readers process logs in parallel safely.  
