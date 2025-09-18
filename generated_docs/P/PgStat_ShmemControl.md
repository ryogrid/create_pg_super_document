# PgStat_ShmemControl

## Location
src/include/utils/pgstat_internal.h: 424 - 455

## Overview
PgStat_ShmemControl is the central shared memory control structure for PostgreSQL's cumulative statistics system, managing both fixed-amount statistics for system components and dynamic hash tables for variable-numbered database objects.

## Definition
```c
typedef struct PgStat_ShmemControl
{
    void           *raw_dsa_area;
    dshash_table_handle hash_handle;        /* shared dbstat hash */
    bool            is_shutdown;
    pg_atomic_uint64 gc_request_count;
    
    /* Stats data for fixed-numbered objects */
    PgStatShared_Archiver archiver;
    PgStatShared_BgWriter bgwriter;
    PgStatShared_Checkpointer checkpointer;
    PgStatShared_IO io;
    PgStatShared_SLRU slru;
    PgStatShared_Wal wal;
} PgStat_ShmemControl;
```

## Detailed Description
This structure serves as the central hub for PostgreSQL's statistics collection infrastructure in shared memory. It coordinates access to both fixed-amount statistics (for system-wide components like the archiver, background writer, checkpointer, etc.) and variable-numbered statistics (for databases, tables, functions, etc.) that are stored in dynamic shared hash tables.

The structure uses dynamic shared memory areas (DSA) to manage variable-sized statistics efficiently. It also includes mechanisms for garbage collection of statistics for dropped objects and provides debugging support for detecting shutdown states. This design allows PostgreSQL to maintain comprehensive performance statistics across all processes while ensuring efficient memory usage and access patterns.

## Parameters / Member Variables
- `raw_dsa_area`: Pointer to the raw dynamic shared area used for managing variable-sized statistics storage
- `hash_handle`: Handle to the shared hash table containing statistics for variable-numbered objects (databases, tables, functions, etc.)
- `is_shutdown`: Boolean flag indicating whether the statistics system has been shut down (used for debugging)
- `gc_request_count`: Atomic counter tracking requests for garbage collection of statistics entries for dropped objects
- `archiver`: Statistics for the WAL archiver process
- `bgwriter`: Statistics for the background writer process
- `checkpointer`: Statistics for the checkpointer process
- `io`: I/O statistics across the system
- `slru`: Statistics for Simple LRU (SLRU) buffer management
- `wal`: Write-Ahead Logging statistics

## Dependencies
- Functions called/Symbols referenced:
  - dshash_table_handle
  - [pg_atomic_uint64](../p/pg_atomic_uint64.md)
  - PgStatShared_Archiver
  - PgStatShared_BgWriter
  - PgStatShared_Checkpointer
  - PgStatShared_IO
  - [PgStatShared_SLRU](PgStatShared_SLRU.md)
  - [PgStatShared_Wal](PgStatShared_Wal.md)
- Called from (representative examples):
  - StatsShmemSize
  - StatsShmemInit
  - pgstat_read_statsfile
  - [PgStat_LocalState](PgStat_LocalState.md)

## Notes and Other Information
- Located in src/include/utils/pgstat_internal.h:424-455
- Central control structure for the entire PostgreSQL statistics system in shared memory
- Manages both fixed-size statistics for system components and dynamic statistics for database objects
- Uses dynamic shared memory areas for efficient memory management of variable-sized statistics
- Includes garbage collection mechanisms to reclaim memory from statistics of dropped objects
- The structure supports PostgreSQL's comprehensive performance monitoring capabilities
- Critical for system observability and performance analysis
- All access to the statistics data goes through this control structure
- The design separates fixed system-wide statistics from dynamic per-object statistics for optimal performance and memory usage