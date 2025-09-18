# TransactionIdDidCommit

## Location
src/backend/access/transam/transam.c: 126 - 187

## Overview
TransactionIdDidCommit determines whether a specified transaction has committed, handling both regular committed transactions and subtransactions through recursive parent checking.

## Definition


## Detailed Description
TransactionIdDidCommit is a core function in PostgreSQL's transaction visibility system that determines if a transaction has successfully committed. The function implements a sophisticated logic to handle different transaction states:

1. **Status Retrieval**: Uses TransactionLogFetch to get the current transaction status
2. **Direct Commit Check**: Returns true immediately if the transaction is marked as TRANSACTION_STATUS_COMMITTED
3. **Subtransaction Handling**: For TRANSACTION_STATUS_SUB_COMMITTED transactions, it recursively checks the parent transaction's commit status
4. **Age-based Optimization**: For old subtransactions (older than TransactionXmin), assumes the parent crashed without cleanup and returns false
5. **Error Handling**: Emits warnings when subtransaction parent information is missing from pg_subtrans

The function is critical for MVCC (Multi-Version Concurrency Control) implementation, determining tuple visibility and transaction consistency throughout the database system.

## Parameters / Member Variables
- : The transaction ID to check for commit status

## Dependencies
- Functions called/Symbols referenced:
  - TransactionLogFetch
  - TransactionIdPrecedes
  - SubTransGetParent
  - TransactionIdIsValid
  - TransactionIdDidCommit (recursive call)
  - XidStatus
  - TRANSACTION_STATUS_COMMITTED
  - TRANSACTION_STATUS_SUB_COMMITTED
- Called from (representative examples):
  - HeapTupleSatisfiesSelf
  - HeapTupleSatisfiesMVCC
  - HeapTupleSatisfiesUpdate
  - HeapTupleSatisfiesDirty
  - FreezeMultiXactId
  - compute_new_xmax_infomask

## Notes and Other Information
- This function assumes the transaction identifier is valid and exists in the commit log (clog)
- Handles subtransaction hierarchies through recursive parent checking
- Used extensively in heap tuple visibility checks across the visibility system
- The recursive nature means it can traverse entire subtransaction trees to determine final commit status
- Critical for determining tuple visibility in MVCC snapshots
- Warning messages indicate potential inconsistencies in pg_subtrans after database startup
- Performance-critical function as it's called frequently during tuple visibility checks