# ResolveRecoveryConflictWithSnapshot

## Location
src/backend/storage/ipc/standby.c: 467 - 510

## Overview
This function generates recovery conflicts to eliminate snapshots that might see transaction IDs at or below a specified conflict horizon as still running, ensuring proper snapshot isolation during standby recovery.

## Definition
```c
void ResolveRecoveryConflictWithSnapshot(TransactionId snapshotConflictHorizon,
                                         bool isCatalogRel,
                                         RelFileLocator locator)
```

## Detailed Description
ResolveRecoveryConflictWithSnapshot implements PostgreSQL's snapshot-based conflict resolution mechanism during WAL replay on standby servers. It identifies and resolves conflicts that arise when standby queries have snapshots that could see transaction IDs as active when those transactions should be considered completed according to the primary server's state.

The function uses snapshotConflictHorizon cutoffs as the standard approach for generating granular recovery conflicts. When a valid transaction ID is provided, it identifies all virtual transactions that have snapshots conflicting with this horizon and resolves them using the ResolveRecoveryConflictWithVirtualXIDs mechanism.

For logical replication scenarios involving catalog relations, the function also invalidates obsolete replication slots to maintain consistency. The function handles several edge cases, including processing InvalidTransactionId values (interpreted as "no conflicts needed") which commonly occur during crash recovery or when replaying already-applied WAL records.

## Parameters / Member Variables
- `snapshotConflictHorizon`: TransactionId representing the conflict boundary - transactions at or below this ID should not be visible to new snapshots
- `isCatalogRel`: Boolean flag indicating whether the operation involves a catalog relation, affecting replication slot invalidation
- `locator`: RelFileLocator containing database OID and other relation identification information

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid
  - TransactionIdIsNormal
  - GetConflictingVirtualXIDs
  - ResolveRecoveryConflictWithVirtualXIDs
  - InvalidateObsoleteReplicationSlots
  - PROCSIG_RECOVERY_CONFLICT_SNAPSHOT
  - WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT
  - RS_INVAL_HORIZON
- Called from (representative examples):
  - gistRedoDeleteRecord
  - hash_xlog_vacuum_one_page
  - heap_xlog_prune_freeze
  - heap_xlog_visible
  - btree_xlog_delete
  - spgRedoVacuumRedirect
  - ResolveRecoveryConflictWithSnapshotFullXid

## Notes and Other Information
- This is a public function (void return type, not static) available for use across the PostgreSQL codebase
- InvalidTransactionId values result in immediate return with no conflict processing
- The function enforces that valid transaction IDs must be normal (not special values like FrozenTransactionId)
- For logical WAL level and catalog relations, replication slot invalidation occurs to maintain logical replication consistency
- Unlike ResolveRecoveryConflictWithVirtualXIDs, this function doesn't directly consider WaitExceedsMaxStandbyDelay since these conflicts should normally be avoided through physical replication slots
- Commonly used during index operations, vacuum processes, and visibility map updates during WAL replay