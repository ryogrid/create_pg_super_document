# AssertTXNLsnOrder

## Location
[src/backend/replication/logical/reorderbuffer.c:938-1008](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L938-L1008)

## Overview
Verifies LSN ordering and other invariants of transaction lists in the reorder buffer during logical replication decoding (debug builds only).

## Definition

```c
static void
AssertTXNLsnOrder(ReorderBuffer *rb)
```
## Detailed Description
This function performs comprehensive validation of LSN ordering and transaction state invariants within the reorder buffer's transaction lists. It operates only in debug builds (when USE_ASSERT_CHECKING is defined) and serves as a critical debugging tool for ensuring the correctness of transaction ordering during logical replication. The function verifies that transactions in the toplevel_by_lsn list are ordered by their first_lsn, and transactions in the txns_by_base_snapshot_lsn list are ordered by their base_snapshot_lsn. It also validates that subtransactions are not incorrectly listed in top-level transaction lists and that LSN relationships within transactions are consistent.

## Parameters / Member Variables
- : The ReorderBuffer containing the transaction lists to be validated

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuildXactNeedsSkip](../S/SnapBuildXactNeedsSkip.md) (checks if transaction decoding should be skipped)
  - dlist_foreach (iterates through transaction lists)
  - dlist_container (extracts transaction from list node)
  - rbtxn_is_known_subxact (checks if transaction is a known subtransaction)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (accessed through rb->private_data)
- Called from (representative examples):
  - [ReorderBufferTXNByXid](../R/ReorderBufferTXNByXid.md) (after adding new top-level transactions)
  - [ReorderBufferAssignChild](../R/ReorderBufferAssignChild.md) (after modifying transaction relationships)
  - [ReorderBufferSetBaseSnapshot](../R/ReorderBufferSetBaseSnapshot.md) (after setting base snapshots)
  - [ReorderBufferGetOldestTXN](../R/ReorderBufferGetOldestTXN.md) (during transaction processing)

## Notes and Other Information
- Only compiled and executed in debug builds (USE_ASSERT_CHECKING must be defined)
- Skips validation before the start_decoding_at LSN since transaction associations may not be complete
- Validates two separate transaction lists: toplevel_by_lsn and txns_by_base_snapshot_lsn
- Ensures strict LSN ordering: each transaction's first_lsn must be higher than the previous transaction's
- Verifies that end_lsn (when set) is always greater than or equal to first_lsn within the same transaction
- Confirms that subtransactions are not mistakenly included in top-level transaction lists
- Validates that transactions with base snapshots have valid base_snapshot_lsn values
- Serves as an essential debugging tool for catching transaction ordering bugs during development
- No-op in production builds, ensuring no performance impact on release versions