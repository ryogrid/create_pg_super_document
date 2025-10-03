# LagTrackerWrite

## Location
[src/backend/replication/walsender.c:4137-4201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L4137-L4201)

## Overview
Records the end of WAL and the time it was flushed locally to enable lag computation when the standby reports receipt of this WAL location.

## Definition

```c
static void
LagTrackerWrite(XLogRecPtr lsn, TimestampTz local_flush_time)
```
## Detailed Description
LagTrackerWrite is a static function in the WAL sender process that maintains a circular buffer of WAL positions and their corresponding flush times. This function is called when new WAL data has been written and flushed locally, storing a sample that will later be used by LagTrackerRead to compute replication lag when the standby reports progress.

The function implements a circular buffer with overflow protection - when the buffer becomes full (when advancing the write head would collide with any read head), it employs a simple adaptive sampling strategy by rewinding and overwriting the previous sample to reduce the sampling rate.

## Parameters / Member Variables
- `lsn`: The WAL log sequence number (XLogRecPtr) representing the end position of newly written WAL data
- `local_flush_time`: The timestamp (TimestampTz) when this WAL data was flushed to disk locally
## Dependencies
- Functions called/Symbols referenced:
  - LAG_TRACKER_BUFFER_SIZE (buffer size constant)
  - NUM_SYNC_REP_WAIT_MODE (number of synchronous replication wait modes)
  - TimeOffset (time-related functionality)
- Called from (representative examples):
  - [XLogSendPhysical](../X/XLogSendPhysical.md) (during physical WAL streaming)
  - WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS (logical replication lag tracking)

## Notes and Other Information
- Only active when am_walsender is true (runs only in WAL sender processes)
- Avoids recording duplicate samples by checking if the LSN has advanced since the last call
- Uses a circular buffer design where the slowest reader (typically the apply process) controls space release
- Implements simple adaptive sampling when buffer becomes full by overwriting the most recent sample
- Part of PostgreSQL's replication lag tracking infrastructure introduced for monitoring standby performance

## Simplified Source

```c
// Simplified version of LagTrackerWrite
static void LagTrackerWrite(XLogRecPtr lsn, TimestampTz local_flush_time) {
    bool buffer_full;
    int new_write_head;
    int i;

    if (!am_walsender)
        return;

    // Skip if LSN hasn't advanced since last time
    if (lag_tracker->last_lsn == lsn)
        return;
    lag_tracker->last_lsn = lsn;

    // Check if advancing write head would collide with any read head
    new_write_head = (lag_tracker->write_head + 1) % LAG_TRACKER_BUFFER_SIZE;
    buffer_full = false;
    for (i = 0; i < NUM_SYNC_REP_WAIT_MODE; ++i) {
        if (new_write_head == lag_tracker->read_heads[i])
            buffer_full = true;
    }

    // If buffer full, rewind and overwrite last sample (adaptive sampling)
    if (buffer_full) {
        new_write_head = lag_tracker->write_head;
        if (lag_tracker->write_head > 0)
            lag_tracker->write_head--;
        else
            lag_tracker->write_head = LAG_TRACKER_BUFFER_SIZE - 1;
    }

    // Store sample at current write head position
    lag_tracker->buffer[lag_tracker->write_head].lsn = lsn;
    lag_tracker->buffer[lag_tracker->write_head].time = local_flush_time;
    lag_tracker->write_head = new_write_head;
}
```

Key simplifications made:
- Function is already well-structured for circular buffer management
- Maintains efficient duplicate detection by LSN comparison
- Preserves adaptive sampling strategy when buffer becomes full
- Essential for replication lag monitoring and performance tuning