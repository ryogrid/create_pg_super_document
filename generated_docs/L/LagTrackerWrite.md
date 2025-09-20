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
- : The WAL log sequence number (XLogRecPtr) representing the end position of newly written WAL data
- : The timestamp (TimestampTz) when this WAL data was flushed to disk locally

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