# LagTracker

## Location
src/backend/replication/walsender.c: 232 - 243

## Overview
LagTracker is a data structure used to track replication lag in PostgreSQL's WAL sender process by maintaining a circular buffer of WAL location and time samples.

## Definition


## Detailed Description
LagTracker implements a mechanism for measuring replication lag between the primary server and standby servers in PostgreSQL streaming replication. It uses a circular buffer to store samples that associate WAL locations (LSNs) with the time they were written locally. The structure supports multiple read heads corresponding to different synchronization modes (write, flush, apply), allowing independent tracking of lag for each replication stage.

The tracker works by recording WAL flush events with their timestamps using LagTrackerWrite(), and then computing elapsed time when standbys report their progress using LagTrackerRead(). It can interpolate between samples to provide lag estimates even when the standby position falls between recorded samples.

## Parameters / Member Variables
- : The last WAL location that was recorded in the tracker, used to avoid duplicate samples
- : Circular buffer of WAL time samples, sized at 8192 entries
- : Current write position in the circular buffer for new samples
- : Array of read positions for different sync replication modes (write, flush, apply)
- : Last sample read for each sync mode, used for interpolation

## Dependencies
- Functions called/Symbols referenced:
  - WalTimeSample (struct for LSN/time pairs)
  - LAG_TRACKER_BUFFER_SIZE (buffer size constant)
  - NUM_SYNC_REP_WAIT_MODE (number of sync modes)
- Called from (representative examples):
  - [LagTrackerWrite](LagTrackerWrite.md) (records new samples)
  - [LagTrackerRead](LagTrackerRead.md) (computes lag from samples)

## Notes and Other Information
- The circular buffer prevents memory growth by overwriting old samples when full
- When buffer is full, the write head is rewound to overwrite the last sample, providing adaptive sampling rate reduction
- Supports interpolation between samples for more accurate lag reporting when standby position falls between recorded points
- Handles clock skew and timeline changes gracefully by returning -1 for invalid scenarios
- Each sync replication mode (write, flush, apply) maintains independent read state for accurate per-stage lag measurement