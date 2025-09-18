# SnapBuildPurgeOlderTxn

## Location
[src/backend/replication/logical/snapbuild.c:1001-1077](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L1001-L1077)

## Overview
Removes outdated transaction information from the snapshot builder's committed and catalog-change tracking arrays to prevent unbounded memory growth.

## Definition
```c
static void SnapBuildPurgeOlderTxn(SnapBuild *builder)
```

## Detailed Description
This function performs crucial memory management for the snapshot building process by purging transaction records that are no longer needed for logical decoding. It removes transactions that are older than the builder's xmin (minimum transaction ID still of interest) from two key tracking arrays.

The function operates in two phases:
1. **Committed transactions purge**: Removes committed transactions older than xmin from the committed.xip array using a copy-and-filter approach
2. **Catalog-change transactions purge**: Removes catalog-modifying transactions older than xmin from the catchange.xip array using an optimized approach that leverages the array's sorted order

The purging is essential because:
- Transactions older than xmin will never be checked via these arrays again
- The clog (commit log) machinery handles visibility checks for these old transactions
- Without purging, these arrays would grow unbounded over time

The catchange array purging is optimized because the array is maintained in sorted order, allowing for efficient binary search-like operations to find the purge boundary.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure containing transaction tracking arrays to be purged

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal (checks if xmin is ready)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (allocates temporary workspace)
  - NormalTransactionIdPrecedes (transaction ID comparison)
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md) (transaction ID comparison)
  - memcpy, memmove, pfree (memory operations)
  - DEBUG3 (logging level)
- Called from (representative examples):
  - SnapBuildProcessRunningXacts (snapbuild.c:1308)

## Notes and Other Information
- Function is declared static, indicating internal use within snapbuild.c
- Uses a temporary workspace for committed transactions to avoid in-place modification complexity
- Leverages sorted order of catchange array for efficient purging using memmove
- Essential for preventing memory leaks in long-running logical replication scenarios
- Only operates when xmin is set to a normal transaction ID
- Provides debug logging to track purging effectiveness
- Part of the snapshot building maintenance infrastructure