# SnapBuildInitialSnapshot

## Location
[src/backend/replication/logical/snapbuild.c:579-677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L579-L677)

## Overview
Builds the initial slot snapshot for logical replication and converts it to a normal MVCC snapshot that can be used by HeapTupleSatisfiesMVCC for consistent data access.

## Definition

```c
Snapshot
SnapBuildInitialSnapshot(SnapBuild *builder)
```
## Detailed Description
This function creates the initial snapshot for a logical replication slot, which establishes a consistent point-in-time view of the database. It performs several critical validations and transformations:

1. **Validation Phase**: Ensures the system is in a proper state for snapshot creation, including checking transaction isolation level (REPEATABLE READ), builder state (SNAPBUILD_CONSISTENT), and that no other snapshots are active.

2. **Snapshot Building**: Uses SnapBuildBuildSnapshot to create the base snapshot, then validates the xmin horizon is properly enforced by checking against the oldest safe decoding transaction ID.

3. **Inversion Process**: Converts the snapbuild's "inverted" representation (where xip contains committed transactions) to the classical snapshot format (where xip contains in-progress transactions). This requires iterating through all transaction IDs from xmin to xmax and building a new xip array.

4. **Resource Management**: Sets MyProc->xmin to enforce the snapshot's xmin horizon and allocates memory in the transaction context for the new xip array.

## Parameters / Member Variables
- : The SnapBuild structure containing the logical decoding state and transaction tracking information

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuildBuildSnapshot](SnapBuildBuildSnapshot.md)
  - [InvalidateCatalogSnapshot](../I/InvalidateCatalogSnapshot.md)
  - HaveRegisteredOrActiveSnapshot
  - HistoricSnapshotActive
  - [GetOldestSafeDecodingTransactionId](../G/GetOldestSafeDecodingTransactionId.md)
  - [GetMaxSnapshotXidCount](../G/GetMaxSnapshotXidCount.md)
  - [TransactionIdFollows](../T/TransactionIdFollows.md)
  - NormalTransactionIdPrecedes
  - TransactionIdAdvance
  - [xidComparator](../x/xidComparator.md)
- Called from (representative examples):
  - [SnapBuildExportSnapshot](SnapBuildExportSnapshot.md)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)

## Notes and Other Information
- Requires REPEATABLE READ isolation level and SNAPBUILD_CONSISTENT state
- Enforces xmin horizon by setting MyProc->xmin to prevent premature cleanup
- The conversion from snapbuild format to MVCC format can be expensive for large transaction ranges
- Includes safeguards against snapshot size limits and serialization failures
- The resulting snapshot has type SNAPSHOT_MVCC and can be used directly or exported for other transactions