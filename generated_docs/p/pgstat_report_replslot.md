# pgstat_report_replslot

## Location
[src/backend/utils/activity/pgstat_replslot.c:78-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_replslot.c#L78-L89)

## Overview
Reports replication slot statistics by accumulating counters from a temporary statistics structure into the shared memory statistics area for a specific replication slot.

## Definition

```c
void
pgstat_report_replslot(ReplicationSlot *slot, const PgStat_StatReplSlotEntry *repSlotStat)
```
## Detailed Description
This function updates the persistent replication slot statistics in shared memory by accumulating values from a temporary statistics structure. It operates on an existing statistics entry that must have been previously created by  or . The function uses a locking mechanism to safely update shared statistics and accumulates various counters related to logical replication decoding activities including spilled transactions, streamed transactions, and total byte counts.

The function employs a macro-based approach () to accumulate multiple statistical fields efficiently. All updates are atomic as the entry reference is locked during the entire operation. This ensures consistency when multiple processes might be accessing replication slot statistics concurrently.

## Parameters / Member Variables
- : Pointer to the ReplicationSlot structure representing the replication slot whose statistics are being updated
- : Pointer to a read-only PgStat_StatReplSlotEntry structure containing the statistical counters to be accumulated into the shared statistics. This structure contains:
  - : Number of transactions that were spilled to disk
  - : Number of individual spill operations
  - : Total bytes spilled to disk
  - : Number of transactions that were streamed
  - : Number of individual streaming operations  
  - : Total bytes streamed
  - : Total number of transactions processed
  - : Total bytes processed

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_entry_ref_locked](pgstat_get_entry_ref_locked.md)
  - [ReplicationSlotIndex](../R/ReplicationSlotIndex.md)
  - [pgstat_unlock_entry](pgstat_unlock_entry.md)
  - PGSTAT_KIND_REPLSLOT
  - [PgStat_EntryRef](../P/PgStat_EntryRef.md)
  - [PgStatShared_ReplSlot](../P/PgStatShared_ReplSlot.md)
  - [PgStat_StatReplSlotEntry](../P/PgStat_StatReplSlotEntry.md)
- Called from (representative examples):
  - [UpdateDecodingStats](../U/UpdateDecodingStats.md) (in logical.c)

## Notes and Other Information
- This function must only be called after the replication slot statistics entry has been initialized via  or 
- The function uses shared memory locking to ensure thread-safe updates to statistics
- Statistics accumulation is performed using the  macro which adds the temporary statistics to the persistent shared memory statistics
- The function is specifically designed for logical replication slot statistics tracking, particularly for monitoring spilling and streaming behavior during logical decoding
- All statistical counters are of type  which provides atomic increment operations
- The locking pattern ensures that statistics updates are consistent and do not interfere with concurrent readers or other statistics reporters