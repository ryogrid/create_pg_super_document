# pgstat_reset_replslot

## Location
[src/backend/utils/activity/pgstat_replslot.c:42-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_replslot.c#L42-L77)

## Overview
Resets statistics counters for a single replication slot, specifically targeting logical replication slots as physical slots do not collect statistics.

## Definition

```c
void
pgstat_reset_replslot(const char *name)
```
## Detailed Description
This function resets the statistics counters for a named replication slot. It first validates that the slot exists by searching for it under a shared lock on the ReplicationSlotControlLock. If the slot is found and it's a logical slot, the function calls pgstat_reset() to clear the statistics. Physical slots are ignored since PostgreSQL only collects statistics for logical replication slots. The function includes proper error handling for non-existent slots.

## Parameters / Member Variables
- `*name`: The name of the replication slot whose statistics should be reset
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (with ReplicationSlotControlLock, LW_SHARED)
  - [SearchNamedReplicationSlot](../S/SearchNamedReplicationSlot.md)
  - SlotIsLogical
  - [pgstat_reset](pgstat_reset.md) (with PGSTAT_KIND_REPLSLOT)
  - [ReplicationSlotIndex](../R/ReplicationSlotIndex.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - ereport (for error handling)
- Called from (representative examples):
  - [pg_stat_reset_replication_slot](pg_stat_reset_replication_slot.md)

## Notes and Other Information
- Only logical replication slots have their statistics reset; physical slots are skipped
- Requires proper permissions through the normal GRANT system
- Uses shared locking to safely access replication slot information
- Throws an error if the specified slot name does not exist
- Part of PostgreSQL's statistics collection system for replication monitoring