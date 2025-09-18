# InvalidateObsoleteReplicationSlots

## Location
[src/backend/replication/slot.c:1775-1834](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1775-L1834)

## Overview
Main entry point for invalidating replication slots that require resources about to be removed, iterating through all slots and applying invalidation criteria.

## Definition
bool InvalidateObsoleteReplicationSlots(ReplicationSlotInvalidationCause cause, XLogSegNo oldestSegno, Oid dboid, TransactionId snapshotConflictHorizon)

## Detailed Description
This function serves as the primary interface for PostgreSQL's replication slot cleanup mechanism. It systematically examines all replication slots in the system and invalidates those that would prevent resource cleanup:

**Invalidation Criteria by Cause:**
- RS_INVAL_WAL_REMOVED: Slots requiring WAL segments older than oldestSegno
- RS_INVAL_HORIZON: Logical slots with snapshot conflicts for the specified database
- RS_INVAL_WAL_LEVEL: All logical slots (when wal_level is insufficient)

**Process Flow:**
1. Converts the oldest WAL segment number to an LSN using XLogSegNoOffsetToRecPtr()
2. Acquires ReplicationSlotControlLock in shared mode
3. Iterates through all replication slots in ReplicationSlotCtl
4. Skips unused slots and logical slots during binary upgrades
5. Calls InvalidatePossiblyObsoleteSlot() for each candidate
6. Restarts iteration if locks were released (indicated by return value)
7. Recalculates resource limits if any slots were invalidated

The function is designed to be safe for use during checkpoints and avoids raising errors when possible.

## Parameters / Member Variables
- cause: The reason for invalidation (determines which slots are candidates)
- oldestSegno: Oldest WAL segment number being retained (for RS_INVAL_WAL_REMOVED)
- dboid: Database OID for horizon conflicts (InvalidOid for shared relations)
- snapshotConflictHorizon: Transaction ID horizon for snapshot conflicts

## Dependencies
- Functions called/Symbols referenced:
  - XLogSegNoOffsetToRecPtr
  - LWLockAcquire/LWLockRelease
  - SlotIsLogical
  - [InvalidatePossiblyObsoleteSlot](InvalidatePossiblyObsoleteSlot.md)
  - [ReplicationSlotsComputeRequiredXmin](../R/ReplicationSlotsComputeRequiredXmin.md)
  - [ReplicationSlotsComputeRequiredLSN](../R/ReplicationSlotsComputeRequiredLSN.md)
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md)
  - [CreateRestartPoint](../C/CreateRestartPoint.md)
  - [xlog_redo](../x/xlog_redo.md)
  - [ResolveRecoveryConflictWithSnapshot](../R/ResolveRecoveryConflictWithSnapshot.md)

## Notes and Other Information
This function is called during critical PostgreSQL operations like checkpoints and standby conflict resolution. It ensures that replication slots don't indefinitely prevent WAL cleanup or block database maintenance operations. The restart mechanism handles the inherent race conditions in slot invalidation, while the final resource limit recalculation ensures the system maintains accurate tracking of retention requirements. Returns true if any slots were invalidated, allowing callers to take appropriate follow-up actions.