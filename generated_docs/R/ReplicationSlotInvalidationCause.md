# ReplicationSlotInvalidationCause

## Location
[src/include/replication/slot.h:56-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/slot.h#L56-L62)

## Overview
An enumeration type that defines the possible causes for replication slot invalidation in PostgreSQL's logical and physical replication system.

## Definition

```c
typedef enum ReplicationSlotInvalidationCause
{
	RS_INVAL_NONE,
	/* required WAL has been removed */
	RS_INVAL_WAL_REMOVED,
	/* required rows have been removed */
	RS_INVAL_HORIZON,
	/* wal_level insufficient for slot */
	RS_INVAL_WAL_LEVEL,
} ReplicationSlotInvalidationCause;
```
## Detailed Description
This enumeration represents the various reasons why a replication slot might become invalidated in PostgreSQL. Replication slots track the progress of logical or physical replication, ensuring that required WAL (Write-Ahead Log) segments and database rows are not removed while they are still needed by replication consumers. When certain conditions occur that make it impossible for a slot to continue functioning properly, the slot is marked as invalidated with one of these specific causes.

The invalidation mechanism helps maintain system stability by preventing infinite accumulation of WAL files or old row versions when replication consumers are not keeping up or have disconnected.

## Parameters / Member Variables
- : No invalidation has occurred; the slot is valid and operational
- : The slot has been invalidated because required WAL segments have been removed (typically due to max_slot_wal_keep_size limit)
- : The slot has been invalidated because required old row versions have been removed by vacuum or other cleanup processes
- : The slot has been invalidated because the current wal_level setting is insufficient to support the slot's requirements

## Dependencies
- Functions called/Symbols referenced:
  - SlotInvalidationCauses (string array mapping enum values to human-readable names)
  - RS_INVAL_MAX_CAUSES (macro defining the maximum cause value)

- Called from (representative examples):
  - [ReportSlotInvalidation](ReportSlotInvalidation.md) (src/backend/replication/slot.c:1477)
  - [InvalidatePossiblyObsoleteSlot](../I/InvalidatePossiblyObsoleteSlot.md) (src/backend/replication/slot.c:1543, 1555, 1562)
  - [InvalidateObsoleteReplicationSlots](../I/InvalidateObsoleteReplicationSlots.md) (src/backend/replication/slot.c:1775)
  - [RestoreSlotFromDisk](RestoreSlotFromDisk.md) (src/backend/replication/slot.c:2404)
  - [GetSlotInvalidationCause](../G/GetSlotInvalidationCause.md) (src/backend/replication/slot.c:2407, 2408)
  - PG_GET_REPLICATION_SLOTS_COLS (src/backend/replication/slotfuncs.c:266)

## Notes and Other Information
- When adding new invalidation causes, developers must update both the SlotInvalidationCauses string array and the RS_INVAL_MAX_CAUSES macro to maintain consistency
- The SlotInvalidationCauses array provides human-readable string representations: "none", "wal_removed", "rows_removed", "wal_level_insufficient"
- A compile-time assertion ensures the array length matches the expected number of causes
- This enumeration is used in the ReplicationSlotPersistentData structure to track the invalidation state of persistent replication slots
- The invalidation state is exposed through system views and functions for monitoring replication slot health