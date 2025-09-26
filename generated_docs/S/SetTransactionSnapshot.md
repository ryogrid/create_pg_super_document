# SetTransactionSnapshot

## Location
[src/backend/utils/time/snapmgr.c:477-573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L477-L573)

## Overview
Sets the transaction's snapshot from an imported MVCC snapshot, ensuring proper isolation and consistency for transaction-snapshot mode operations.

## Definition

```c
static void
SetTransactionSnapshot(Snapshot sourcesnap, VirtualTransactionId *sourcevxid,
					   int sourcepid, PGPROC *sourceproc)
```
## Detailed Description
This function establishes a transaction's snapshot by importing an MVCC snapshot from another transaction or process. It handles the complex task of safely transferring snapshot state while maintaining ACID properties and preventing global xmin from moving backwards. The function is closely tied to GetTransactionSnapshot and must handle all the same considerations as the first-snapshot case.

The process involves:
1. Validating preconditions (no existing snapshots)
2. Calling GetSnapshotData to ensure proper initialization
3. Copying snapshot fields from the source
4. Atomically installing the xmin to prevent race conditions
5. Handling serializable isolation requirements
6. Registering the snapshot for transaction-snapshot mode

## Parameters / Member Variables
- : The source snapshot to import, containing xmin, xmax, active transaction arrays, and other MVCC state
- : Virtual transaction ID of the source transaction (used when sourceproc is NULL)
- : Process ID of the source process (for error reporting)
- : PGPROC structure of the source process (used for direct validation, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - InvalidateCatalogSnapshot
  - pairingheap_is_empty
  - HistoricSnapshotActive
  - GetSnapshotData
  - GetMaxSnapshotXidCount
  - GetMaxSnapshotSubxidCount
  - ProcArrayInstallRestoredXmin
  - ProcArrayInstallImportedXmin
  - IsolationUsesXactSnapshot
  - IsolationIsSerializable
  - SetSerializableTransactionSnapshot
  - CopySnapshot
  - pairingheap_add
- Called from (representative examples):
  - ImportSnapshot
  - RestoreTransactionSnapshot

## Notes and Other Information
- This is a static function in snapmgr.c, not exposed as a public API
- The function includes race condition protection when installing the xmin value
- In serializable mode, additional processing by predicate.c occurs
- The curcid (current command ID) is intentionally NOT copied as it's transaction-local
- For transaction-snapshot isolation levels, the snapshot is copied and registered to persist until transaction end
- Caller must ensure FirstSnapshotSet is false before calling this function