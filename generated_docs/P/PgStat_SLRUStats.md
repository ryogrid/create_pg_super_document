# PgStat_SLRUStats

## Location
[src/include/pgstat.h:380-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L380-L390)

## Overview
PgStat_SLRUStats is a structure that tracks statistics for Simple Least Recently Used (SLRU) buffer caches in PostgreSQL, monitoring block operations, cache performance, and maintenance activities for various internal cache systems.

## Definition

```c
typedef struct PgStat_SLRUStats
{
	PgStat_Counter blocks_zeroed;
	PgStat_Counter blocks_hit;
	PgStat_Counter blocks_read;
	PgStat_Counter blocks_written;
	PgStat_Counter blocks_exists;
	PgStat_Counter flush;
	PgStat_Counter truncate;
	TimestampTz stat_reset_timestamp;
} PgStat_SLRUStats;
```
## Detailed Description
PgStat_SLRUStats maintains performance statistics for PostgreSQL's SLRU (Simple Least Recently Used) buffer cache system. SLRU caches are used throughout PostgreSQL for various purposes including commit logs (CLOG), subtransaction logs (SUBTRANS), multixact members and offsets, and other internal data structures. This structure tracks various block-level operations and cache behavior, providing insights into the efficiency and activity of these critical internal cache systems.

## Parameters / Member Variables
- : Number of blocks that were zeroed (newly initialized) in the SLRU cache
- : Number of cache hits when accessing blocks in the SLRU cache
- : Number of blocks that were read from disk into the SLRU cache
- : Number of blocks that were written from the SLRU cache to disk
- : Number of times a block was found to already exist during operations
- : Number of flush operations performed on the SLRU cache
- : Number of truncate operations performed on the SLRU cache
- : Timestamp indicating when the statistics for this SLRU cache were last reset

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (statistics counter type)
  - TimestampTz (timestamp data type)
- Called from (representative examples):
  - [pgstat_count_slru_truncate](../p/pgstat_count_slru_truncate.md) (SLRU truncation statistics)
  - pgstat_slru_flush (SLRU flush operation statistics)
  - [pgstat_slru_snapshot_cb](../p/pgstat_slru_snapshot_cb.md) (statistics snapshot callback)
  - [pgstat_reset_slru_counter_internal](../p/pgstat_reset_slru_counter_internal.md) (statistics reset function)
  - PG_STAT_GET_SLRU_COLS (SQL interface macro)
  - pgstat_count_buffer_hit (buffer statistics integration)
  - [PgStatShared_SLRU](PgStatShared_SLRU.md) (shared memory statistics structure)
  - [PgStat_Snapshot](PgStat_Snapshot.md) (statistics snapshot structure)

## Notes and Other Information
- This structure is the foundation for PostgreSQL's pg_stat_slru system view
- SLRU caches are used for critical PostgreSQL subsystems including CLOG, SUBTRANS, MULTIXACT, and others
- Cache hit statistics help assess the efficiency of SLRU buffer management
- Block read/write statistics indicate I/O pressure on SLRU-managed data
- Flush and truncate statistics show maintenance activity levels
- Different SLRU caches (CLOG, SUBTRANS, etc.) maintain separate statistics instances
- Used for monitoring internal PostgreSQL subsystem performance
- Critical for understanding transaction log and metadata access patterns
- Statistics help identify when SLRU cache sizes might need adjustment
- The blocks_zeroed counter indicates creation of new cache pages
- High read-to-hit ratios may suggest insufficient SLRU cache sizing for the workload