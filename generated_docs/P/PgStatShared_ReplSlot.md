# PgStatShared_ReplSlot

## Location
src/include/utils/pgstat_internal.h: 410 - 414

## Overview
PgStatShared_ReplSlot represents shared memory statistics data for PostgreSQL replication slots, tracking various metrics related to transaction spilling, streaming, and overall replication slot activity.

## Definition
```c
typedef struct PgStatShared_ReplSlot
{
    PgStatShared_Common header;
    PgStat_StatReplSlotEntry stats;
} PgStatShared_ReplSlot;
```

## Detailed Description
This structure stores replication slot statistics in PostgreSQL's shared memory statistics system. It combines the standard common header used by all shared statistics objects with replication slot-specific performance and activity data. The structure is essential for monitoring the health and performance of logical replication slots, particularly tracking how much data is being spilled to disk versus streamed directly, and overall transaction processing metrics.

Replication slots are a critical component of PostgreSQL's logical replication system, and this statistics structure helps database administrators monitor their performance, identify bottlenecks, and troubleshoot replication issues.

## Parameters / Member Variables
- `header`: Common header structure (PgStatShared_Common) containing magic number for validation and an LWLock for protecting access to the statistics data
- `stats`: Replication slot-specific statistics (PgStat_StatReplSlotEntry) containing various counters for spill operations, stream operations, total transactions, and reset timestamp

## Dependencies
- Functions called/Symbols referenced:
  - PgStatShared_Common
  - PgStat_StatReplSlotEntry
- Called from (representative examples):
  - pgstat_report_replslot
  - pgstat_create_replslot
  - pgstat_replslot_reset_timestamp_cb
  - SH_DECLARE (hash table declarations in pgstat.c)

## Notes and Other Information
- Located in src/include/utils/pgstat_internal.h:410-414
- Part of PostgreSQL's shared memory statistics infrastructure for tracking replication slot performance
- The statistics include detailed metrics about spill operations (when changes are written to disk) and stream operations (when changes are streamed directly)
- Tracks both transaction counts and byte counts for comprehensive monitoring
- Used within hash tables for efficient lookup of replication slot statistics by slot name or OID
- Access to the statistics data is protected by the LWLock in the header to ensure thread-safe operations
- Critical for monitoring logical replication performance and diagnosing replication bottlenecks
- The structure supports PostgreSQL's logical replication monitoring capabilities, helping administrators understand replication slot behavior and performance characteristics