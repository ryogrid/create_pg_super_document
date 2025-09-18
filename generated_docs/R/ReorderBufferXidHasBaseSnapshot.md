# ReorderBufferXidHasBaseSnapshot

## Location
[src/backend/replication/logical/reorderbuffer.c:3620-3649](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3620-L3649)

## Overview
Checks whether a transaction or subtransaction has already been assigned a base snapshot for logical replication processing.

## Definition
bool ReorderBufferXidHasBaseSnapshot(ReorderBuffer *rb, TransactionId xid)

## Detailed Description
This function determines whether a given transaction has already been assigned a base snapshot, which is essential for logical replication. Base snapshots define the visibility rules for a transaction during logical decoding. The function handles both regular transactions and subtransactions by redirecting subtransaction queries to their top-level transaction, since snapshots are managed at the top-level transaction level. If the transaction is not known to the reorder buffer, the function returns false.

## Parameters / Member Variables
- rb: Pointer to the ReorderBuffer structure to search
- xid: Transaction ID to check for base snapshot presence

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - rbtxn_is_known_subxact
- Called from (representative examples):
  - [SnapBuildProcessChange](../S/SnapBuildProcessChange.md)
  - [SnapBuildDistributeSnapshotAndInval](../S/SnapBuildDistributeSnapshotAndInval.md)
  - SnapBuildCommitTxn

## Notes and Other Information
- Returns false if the transaction is not found in the reorder buffer
- For subtransactions, the function checks the top-level transaction's base snapshot since snapshots are managed at that level
- Base snapshots are crucial for determining which tuples should be visible during logical replication replay
- Used by snapshot building logic to coordinate snapshot distribution and transaction processing
- Part of PostgreSQL's logical replication infrastructure for managing transaction visibility