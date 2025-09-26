# SnapBuildCommitTxn

## Location
[src/backend/replication/logical/snapbuild.c:1078-1243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L1078-L1243)

## Overview
Handles all necessary processing when a transaction commits in the logical replication snapshot building context, managing snapshot state transitions, catalog change tracking, and timeline visibility.

## Definition

```c
void
SnapBuildCommitTxn(SnapBuild *builder, XLogRecPtr lsn, TransactionId xid,
				   int nsubxacts, TransactionId *subxacts, uint32 xinfo)
```
## Detailed Description
SnapBuildCommitTxn is a central function in PostgreSQL's logical replication snapshot building mechanism. It processes transaction commits and determines their impact on the evolving snapshot state. The function handles multiple scenarios based on the builder's current state:

1. **Early State Filtering**: Transactions preceding the BUILDING_SNAPSHOT phase are ignored as they won't be decoded or included in snapshots.

2. **Catalog Change Detection**: Uses SnapBuildXidHasCatalogChanges to identify transactions that modified system catalogs, which require special handling for consistent snapshot building.

3. **Subtransaction Processing**: Iterates through all subtransactions, adding catalog-modifying ones to the committed transaction set and tracking their visibility requirements.

4. **Timeline Management**: Manages timetravel requirements for maintaining historical visibility and adjusts the builder's xmax to encompass all relevant committed transactions.

5. **Snapshot Distribution**: When necessary, builds and distributes new snapshots to maintain consistency across the replication system.

The function ensures that only transactions relevant to logical replication are tracked while maintaining proper visibility semantics for historical snapshot reconstruction.

## Parameters / Member Variables
- : The SnapBuild context containing the current snapshot building state
- : Log sequence number where the commit was logged
- : Transaction ID of the committing transaction
- : Number of subtransactions in the subxacts array
- : Array of subtransaction IDs that are part of this commit
- : Additional transaction information flags

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdPrecedes
  - SnapBuildXidHasCatalogChanges
  - SnapBuildAddCommittedTxn
  - NormalTransactionIdFollows
  - TransactionIdFollowsOrEquals
  - TransactionIdAdvance
  - SnapBuildSnapDecRefcount
  - SnapBuildBuildSnapshot
  - ReorderBufferXidHasBaseSnapshot
  - SnapBuildSnapIncRefcount
  - ReorderBufferSetBaseSnapshot
  - SnapBuildDistributeSnapshotAndInval
- Called from (representative examples):
  - DecodeCommit

## Notes and Other Information
- Critical for maintaining snapshot consistency during logical replication setup
- Handles complex state transitions in the snapshot building process (START → BUILDING_SNAPSHOT → CONSISTENT → FULL_SNAPSHOT)
- Must carefully track catalog-modifying transactions to ensure schema changes are properly reflected
- Manages reference counting for snapshot objects to prevent memory leaks
- Uses debugging output levels (DEBUG1, DEBUG2) for transaction tracking diagnostics
- The function's behavior changes significantly based on the builder->state, making it a state-machine-driven operation