# TransactionIdIsActive

## Location
[src/backend/storage/ipc/procarray.c:1634-1734](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L1634-L1734)

## Overview
TransactionIdIsActive determines whether a transaction ID corresponds to the top-level XID of an active backend, excluding prepared transactions and subtransactions.

## Definition
```c
bool TransactionIdIsActive(TransactionId xid)
```

## Detailed Description
This function provides a simplified and more restrictive check compared to TransactionIdIsInProgress. It specifically identifies whether a given transaction ID belongs to an actively running backend process, with several important exclusions:

**Key differences from TransactionIdIsInProgress:**
1. **Ignores prepared transactions**: Transactions that have been prepared for two-phase commit but not yet committed/aborted are not considered "active"
2. **Ignores Hot Standby transactions**: Does not check KnownAssignedXids for transactions running on the primary server
3. **Top-level XIDs only**: Does not search subtransactions or cached subxids
4. **No pg_subtrans lookup**: Does not perform expensive subtransaction tree traversal

**Algorithm:**
1. Quick rejection of transactions older than RecentXmin
2. Linear scan through ProcGlobal->xids array
3. Skip processes with pid == 0 (prepared transactions)
4. Direct comparison with main transaction IDs only

This simplified approach makes the function significantly faster than TransactionIdIsInProgress but less comprehensive. It's designed for specific use cases where only active, non-prepared, top-level transactions matter.

## Parameters / Member Variables
- `xid`: The transaction ID to check for active status in running backends

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdPrecedes](TransactionIdPrecedes.md) (for age-based quick rejection)
  - TransactionIdEquals (for exact XID matching)
  - UINT32_ACCESS_ONCE (for atomic XID access)
  - [ProcArrayStruct](../P/ProcArrayStruct.md) (access to process array structure)
- Called from:
  - Currently only referenced in header file, suggesting limited specialized usage

## Notes and Other Information
- Much simpler and faster than TransactionIdIsInProgress due to fewer checks
- Designed for scenarios where prepared transactions should be treated as inactive
- Does not handle subtransactions, making it unsuitable for general visibility checks
- Uses shared ProcArrayLock for safe concurrent access to process array
- The pid == 0 check effectively filters out prepared transactions
- No caching mechanism since the function is relatively lightweight
- Atomic access to XIDs prevents torn reads during concurrent transaction ID updates
- Primarily used internally where specific semantics (active backends only) are required