# PgStat_Snapshot

## Location
[src/include/utils/pgstat_internal.h:461-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L461-L485)

## Overview
PgStat_Snapshot represents a cached statistics snapshot that provides a consistent view of PostgreSQL system statistics at a specific point in time, containing both fixed system-wide statistics and variable per-object statistics.

## Definition
```c
typedef struct PgStat_Snapshot
{
    PgStat_FetchConsistency mode;
    TimestampTz snapshot_timestamp;
    bool        fixed_valid[PGSTAT_NUM_KINDS];
    
    /* Fixed system-wide statistics */
    PgStat_ArchiverStats archiver;
    PgStat_BgWriterStats bgwriter;
    PgStat_CheckpointerStats checkpointer;
    PgStat_IO   io;
    PgStat_SLRUStats slru[SLRU_NUM_ELEMENTS];
    PgStat_WalStats wal;
    
    /* Memory management and variable statistics */
    MemoryContext context;
    struct pgstat_snapshot_hash *stats;
} PgStat_Snapshot;
```

## Detailed Description
This structure provides a consistent, point-in-time snapshot of PostgreSQL's statistics data. It captures both fixed system-wide statistics (like archiver, background writer, checkpointer activity) and variable per-object statistics (databases, tables, functions, etc.) in a single coherent view. The snapshot mechanism is crucial for ensuring consistency when reading multiple related statistics that could otherwise change between individual reads.

The structure includes validation flags to track which fixed statistics are valid in the snapshot, a timestamp indicating when the snapshot was taken, and a memory context for efficient bulk memory management. This design allows applications and monitoring tools to get a consistent view of system performance without worrying about statistics changing during the read process.

## Parameters / Member Variables
- `mode`: Consistency mode specifying how the snapshot was fetched (PgStat_FetchConsistency)
- `snapshot_timestamp`: Timestamp indicating when this snapshot was captured
- `fixed_valid`: Array of boolean flags indicating which fixed statistics types are valid in this snapshot
- `archiver`: Cached statistics for the WAL archiver process
- `bgwriter`: Cached statistics for the background writer process
- `checkpointer`: Cached statistics for the checkpointer process
- `io`: Cached I/O statistics across the system
- `slru`: Array of cached statistics for all SLRU (Simple LRU) buffer pools
- `wal`: Cached Write-Ahead Logging statistics
- `context`: Memory context used for allocating snapshot data, enabling efficient bulk cleanup
- `stats`: Hash table containing variable statistics for databases, tables, functions, and other dynamic objects

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_FetchConsistency
  - PGSTAT_NUM_KINDS
  - PgStat_ArchiverStats
  - PgStat_BgWriterStats
  - PgStat_CheckpointerStats
  - [PgStat_IO](PgStat_IO.md)
  - [PgStat_SLRUStats](PgStat_SLRUStats.md)
  - SLRU_NUM_ELEMENTS
  - [PgStat_WalStats](PgStat_WalStats.md)
- Called from (representative examples):
  - [PgStat_LocalState](PgStat_LocalState.md)

## Notes and Other Information
- Located in src/include/utils/pgstat_internal.h:461-485
- Provides a consistent, point-in-time view of all PostgreSQL statistics
- Essential for monitoring tools and applications that need coherent statistics data
- Uses memory contexts for efficient memory management and bulk cleanup of snapshot data
- The fixed_valid array ensures that only valid statistics are included in the snapshot
- Supports different consistency modes for balancing performance and consistency requirements
- Critical component of PostgreSQL's statistics infrastructure, enabling reliable performance monitoring
- The snapshot mechanism prevents inconsistencies that could occur when statistics change during multi-step read operations
- Used by various PostgreSQL components and extensions that need reliable access to system statistics