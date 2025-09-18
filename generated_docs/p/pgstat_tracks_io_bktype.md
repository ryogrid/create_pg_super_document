# pgstat_tracks_io_bktype

## Location
src/backend/utils/activity/pgstat_io.c: 319 - 358

## Overview
This function determines whether I/O statistics are collected for a specific backend type by returning true for backend types that participate in the cumulative stats subsystem.

## Definition


## Detailed Description
The `pgstat_tracks_io_bktype` function acts as a filter to determine which backend types should have their I/O operations tracked in PostgreSQL's statistics system. Not all backend types participate in I/O statistics collection due to various reasons:

- Some backends (like Syslogger) are not connected to shared memory
- Others (like Archiver) delegate most I/O to specialized commands
- WAL-related processes (WAL Receiver, WAL Writer, WAL Summarizer) currently do not have their I/O tracked in pg_stat_io

The function uses an explicit switch statement that lists every backend type, ensuring that new backend types trigger compiler warnings about needing to update this function.

## Parameters / Member Variables
- `bktype`: The BackendType enum value to check for I/O tracking eligibility

## Dependencies
- Functions called/Symbols referenced:
  - BackendType (enum type)
  - B_INVALID, B_ARCHIVER, B_LOGGER, B_WAL_RECEIVER, B_WAL_WRITER, B_WAL_SUMMARIZER (backend types that don't track I/O)
  - B_AUTOVAC_LAUNCHER, B_AUTOVAC_WORKER, B_BACKEND, B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_SLOTSYNC_WORKER, B_STANDALONE_BACKEND, B_STARTUP, B_WAL_SENDER (backend types that do track I/O)
- Called from (representative examples):
  - pgstat_tracks_io_object() (to validate backend type before checking object tracking)
  - pg_stat_get_io() (to filter backends when retrieving I/O statistics)

## Notes and Other Information
- Returns false for backend types that do not participate in I/O tracking: B_INVALID, B_ARCHIVER, B_LOGGER, B_WAL_RECEIVER, B_WAL_WRITER, B_WAL_SUMMARIZER
- Returns true for backend types that do participate in I/O tracking: B_AUTOVAC_LAUNCHER, B_AUTOVAC_WORKER, B_BACKEND, B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_SLOTSYNC_WORKER, B_STANDALONE_BACKEND, B_STARTUP, B_WAL_SENDER
- The explicit switch statement design ensures compiler warnings when new backend types are added
- When adding new backend types, developers should also consider updating pgstat_tracks_io_object() and pgstat_tracks_io_op()
- Located in src/backend/utils/activity/pgstat_io.c:319-358