# PgStatShared_SLRU

## Location
[src/include/utils/pgstat_internal.h:362-367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L362-L367)

## Overview
A shared memory structure that holds SLRU (Simple Least Recently Used) statistics for all SLRU types in PostgreSQL, protected by a lightweight lock for concurrent access.

## Definition

```c
typedef struct PgStatShared_SLRU
{
	/* lock protects ->stats */
	LWLock		lock;
	PgStat_SLRUStats stats[SLRU_NUM_ELEMENTS];
} PgStatShared_SLRU;
```
## Detailed Description
PgStatShared_SLRU is a shared memory structure that maintains statistics for all SLRU (Simple Least Recently Used) caches in PostgreSQL. SLRUs are used for various system components like transaction logs, multixact data, commit timestamps, and notifications. This structure provides a centralized location to collect and access performance statistics for these critical system components.

The structure is designed for concurrent access in a multi-process environment, using an LWLock to protect the statistics array from race conditions during updates and reads. Each SLRU type has its own dedicated statistics entry in the array, indexed by predefined SLRU identifiers.

## Parameters / Member Variables
- `lock`: LWLock that protects concurrent access to the stats array, ensuring data consistency during statistics updates and reads
- `stats[SLRU_NUM_ELEMENTS]`: Array of PgStat_SLRUStats structures, one for each SLRU type (commit_timestamp, multixact_member, multixact_offset, notify, serializable, subtransaction, transaction, and other)
## Dependencies
- Functions called/Symbols referenced:
  - [LWLock](../L/LWLock.md)
  - [PgStat_SLRUStats](PgStat_SLRUStats.md)
  - SLRU_NUM_ELEMENTS
- Called from (representative examples):
  - [pgstat_slru_flush](../p/pgstat_slru_flush.md)
  - [pgstat_slru_snapshot_cb](../p/pgstat_slru_snapshot_cb.md)
  - [pgstat_reset_slru_counter_internal](../p/pgstat_reset_slru_counter_internal.md)
  - [PgStat_ShmemControl](PgStat_ShmemControl.md) (as a member)

## Notes and Other Information
- SLRU_NUM_ELEMENTS is defined as lengthof(slru_names) where slru_names contains 8 SLRU types: commit_timestamp, multixact_member, multixact_offset, notify, serializable, subtransaction, transaction, and other
- The 'other' SLRU type serves as a catch-all for SLRUs without explicit entries, including those from extensions
- This structure is part of the larger PostgreSQL statistics collection system and is embedded within PgStat_ShmemControl
- Statistics tracked include blocks_zeroed, blocks_hit, blocks_read, blocks_written, blocks_exists, flush, truncate operations, and reset timestamp for each SLRU type