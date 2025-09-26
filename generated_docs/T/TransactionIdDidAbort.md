# TransactionIdDidAbort

## Location
[src/backend/access/transam/transam.c:188-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/transam.c#L188-L239)

## Overview
TransactionIdDidAbort determines whether a specified transaction has explicitly aborted, with special handling for subtransactions and crash scenarios.

## Definition

```c
bool							/* true if given transaction aborted */
TransactionIdDidAbort(TransactionId transactionId)
```
## Detailed Description
TransactionIdDidAbort is a specialized function that determines if a transaction has explicitly aborted. Unlike commit checking, abort detection has additional complexities due to crash scenarios:

1. **Status Retrieval**: Uses TransactionLogFetch to get the current transaction status
2. **Direct Abort Check**: Returns true immediately if the transaction is marked as TRANSACTION_STATUS_ABORTED
3. **Subtransaction Handling**: For TRANSACTION_STATUS_SUB_COMMITTED transactions, recursively checks the parent's abort status
4. **Crash Assumption**: For old subtransactions (older than TransactionXmin), assumes the parent crashed and returns true (assuming abort)
5. **Error Handling**: Emits warnings when subtransaction parent information is missing and assumes abort

The function is particularly important for distinguishing between transactions that explicitly aborted versus those that appear in-progress due to crashes. This distinction is crucial for proper cleanup and consistency checking.

## Parameters / Member Variables
- : The transaction ID to check for abort status

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionLogFetch](TransactionLogFetch.md)
  - [TransactionIdPrecedes](TransactionIdPrecedes.md)  
  - [SubTransGetParent](../S/SubTransGetParent.md)
  - TransactionIdIsValid
  - [TransactionIdDidAbort](TransactionIdDidAbort.md) (recursive call)
  - XidStatus
  - TRANSACTION_STATUS_ABORTED
  - TRANSACTION_STATUS_SUB_COMMITTED
- Called from (representative examples):
  - [heap_update](../h/heap_update.md)
  - [test_lockmode_for_conflict](../t/test_lockmode_for_conflict.md)
  - [heap_lock_updated_tuple_rec](../h/heap_lock_updated_tuple_rec.md)
  - [DoesMultiXactIdConflict](../D/DoesMultiXactIdConflict.md)
  - [TransactionIdIsInProgress](TransactionIdIsInProgress.md)

## Notes and Other Information
- Assumes the transaction identifier is valid and exists in the commit log (clog)
- Returns true only for explicitly aborted transactions, not crash-induced aborts
- Transactions that crashed may still appear in-progress in clog rather than aborted
- Most code should use TransactionIdDidCommit() with TransactionIdIsInProgress() checks instead
- The function's behavior for old subtransactions differs from TransactionIdDidCommit - it assumes abort rather than non-commit
- Used less frequently than TransactionIdDidCommit due to the ambiguity around crash vs explicit abort
- Critical for lock conflict detection and tuple update operations