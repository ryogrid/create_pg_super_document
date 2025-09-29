# LagTrackerRead

## Location
[src/backend/replication/walsender.c:4202-4297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L4202-L4297)

## Overview
Computes the time elapsed between when a WAL location was flushed locally and a given timestamp, using data stored by LagTrackerWrite to measure replication lag.

## Definition
```c
static TimeOffset LagTrackerRead(int head, XLogRecPtr lsn, TimestampTz now)
```

## Detailed Description
LagTrackerRead calculates replication lag by finding the time difference between when WAL data was originally flushed locally and the current time. It maintains separate read heads for different types of standby progress reports (write, flush, apply) and processes samples from the circular buffer created by LagTrackerWrite.

The function implements sophisticated lag computation logic including interpolation between samples when exact matches aren't available. It handles edge cases like clock adjustments, timeline changes, and periods of inactivity. When the standby hasn't reached a recorded sample yet, it can interpolate between the last processed sample and the next available one to provide more accurate lag estimates.

## Parameters / Member Variables
- `head`: Index indicating which read head to use (corresponds to different replication progress types: write, flush, or apply)
- `lsn`: The WAL log sequence number reported by the standby for which lag should be calculated
- `now`: Current timestamp to compute lag against

## Dependencies
- Functions called/Symbols referenced:
  - LAG_TRACKER_BUFFER_SIZE (circular buffer size constant)
  - WalTimeSample (structure for storing LSN/timestamp pairs)
- Called from (representative examples):
  - [ProcessStandbyReplyMessage](../P/ProcessStandbyReplyMessage.md) (multiple calls for different progress types)

## Notes and Other Information
- Returns -1 when no lag data is available or when clock inconsistencies are detected
- Returns lag time in microseconds when successful
- Implements linear interpolation between samples to provide lag estimates even when exact LSN matches aren't found
- Handles clock backwards movement gracefully by treating such cases as data unavailable
- Automatically clears stale data when the standby has processed all available WAL to prevent using irrelevant samples after idle periods
- Part of PostgreSQL's comprehensive replication monitoring system that provides separate lag measurements for write, flush, and apply operations

## Simplified Source

```c
static TimeOffset LagTrackerRead(int head, XLogRecPtr lsn, TimestampTz now) {
    TimestampTz time = 0;

    // Read all samples up to the requested LSN
    while (lag_tracker->read_heads[head] != lag_tracker->write_head &&
           lag_tracker->buffer[lag_tracker->read_heads[head]].lsn <= lsn) {
        time = lag_tracker->buffer[lag_tracker->read_heads[head]].time;
        lag_tracker->last_read[head] = lag_tracker->buffer[lag_tracker->read_heads[head]];
        lag_tracker->read_heads[head] = (lag_tracker->read_heads[head] + 1) % LAG_TRACKER_BUFFER_SIZE;
    }

    // Clear stale data if buffer is empty
    if (lag_tracker->read_heads[head] == lag_tracker->write_head)
        lag_tracker->last_read[head].time = 0;

    // Handle clock going backwards
    if (time > now)
        return -1;

    // If no direct sample, try interpolation
    if (time == 0) {
        if (lag_tracker->read_heads[head] == lag_tracker->write_head) {
            // No future samples - cannot interpolate
            return -1;
        } else if (lag_tracker->last_read[head].time != 0) {
            // Interpolate between last_read and next sample
            double fraction;
            WalTimeSample prev = lag_tracker->last_read[head];
            WalTimeSample next = lag_tracker->buffer[lag_tracker->read_heads[head]];

            // Validate LSN ordering
            if (lsn < prev.lsn || prev.time > next.time)
                return -1;

            // Calculate proportional time
            fraction = (double)(lsn - prev.lsn) / (double)(next.lsn - prev.lsn);
            time = (TimestampTz)((double)prev.time + (next.time - prev.time) * fraction);
        } else {
            // Use future sample as hypothetical lag
            time = lag_tracker->buffer[lag_tracker->read_heads[head]].time;
        }
    }

    // Return elapsed time in microseconds
    Assert(time != 0);
    return now - time;
}
```