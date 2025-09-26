# GetLatestSnapshot

## Location
src/backend/utils/time/snapmgr.c: 291 - 322

## Overview
Obtains a snapshot that reflects the most current database state, even when executing in transaction-snapshot isolation mode.

## Definition
```c
Snapshot GetLatestSnapshot(void)
```

## Detailed Description
GetLatestSnapshot provides access to the most up-to-date view of the database, bypassing the transaction-snapshot isolation mechanism when necessary. Unlike GetTransactionSnapshot, which may return a fixed snapshot for the entire transaction in certain isolation levels, this function always returns a snapshot that reflects the current state of committed transactions. This is essential for operations that need to see the latest committed data regardless of the transactions isolation level, such as foreign key validation, constraint checking, and certain system catalog operations.

The function uses a secondary snapshot storage area (SecondarySnapshot/SecondarySnapshotData) to avoid interfering with the main transaction snapshot. If called as the first snapshot in a transaction, it delegates to GetTransactionSnapshot for initialization.

## Parameters / Member Variables
- Returns: A Snapshot representing the most current view of the database

## Dependencies
- Functions called/Symbols referenced:
  - IsInParallelMode
  - HistoricSnapshotActive
  - GetTransactionSnapshot
  - GetSnapshotData
- Called from (representative examples):
  - IndexCheckExclusion
  - asyncQueueReadAllNotifications
  - ATRewriteTable
  - validateForeignKeyConstraint
  - RI_Initial_Check
  - ri_PerformCheck

## Notes and Other Information
- Explicitly prohibited during parallel operations for consistency
- Not currently supported during logical decoding (assertion enforced)
- Uses SecondarySnapshot storage to avoid conflicts with transaction snapshots
- Critical for operations requiring fresh visibility of committed data
- Often used in constraint validation and referential integrity checks
- Bypasses transaction-snapshot mode restrictions when current data visibility is essential
- The returned snapshot reflects the database state at the moment of the call