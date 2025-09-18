# GetWALAvailability

## Location
[src/backend/access/transam/xlog.c:7881-7966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L7881-L7966)

## Overview
GetWALAvailability reports the availability status of WAL segments for a given target LSN, typically used to determine if a replication slot's restart_lsn is still accessible.

## Definition
```c
WALAvailability GetWALAvailability(XLogRecPtr targetLSN)
```

## Detailed Description
This function analyzes WAL segment availability for a specific LSN (typically a replication slot's restart_lsn) by examining current WAL position, retention policies, and segment cleanup status. It categorizes availability into five distinct states based on various retention boundaries including max_wal_size and slot-based retention.

The function calculates several key segment boundaries:
- Current segment based on write position
- Oldest segment retained by replication slots
- Oldest segment retained by max_wal_size policy
- Oldest extant segment file on disk
- Target segment containing the requested LSN

Based on these boundaries, it determines the appropriate availability status.

## Parameters / Member Variables
- `targetLSN`: The LSN to check availability for, typically a replication slot's restart_lsn

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid
  - [GetXLogWriteRecPtr](GetXLogWriteRecPtr.md)
  - [XLogGetReplicationSlotMinimumLSN](../X/XLogGetReplicationSlotMinimumLSN.md)
  - [KeepLogSeg](../K/KeepLogSeg.md)
  - [XLogGetLastRemovedSegno](../X/XLogGetLastRemovedSegno.md)
  - XLByteToSeg
  - ConvertToXSegs
- Called from (representative examples):
  - PG_GET_REPLICATION_SLOTS_COLS (for pg_replication_slots view)

## Notes and Other Information
- Returns WALAvailability enum with five possible values:
  - WALAVAIL_RESERVED: Available within max_wal_size range
  - WALAVAIL_EXTENDED: Available beyond max_wal_size due to slot retention
  - WALAVAIL_UNRESERVED: Being lost, will be removed at next checkpoint
  - WALAVAIL_REMOVED: Already removed, replication cannot continue
  - WALAVAIL_INVALID_LSN: Invalid LSN provided (slot not set to reserve WAL)
- Used primarily for monitoring replication slot health and WAL retention
- Critical for determining if replication streams can continue or need to be reestablished
- The function provides early warning when WAL segments are at risk of being removed