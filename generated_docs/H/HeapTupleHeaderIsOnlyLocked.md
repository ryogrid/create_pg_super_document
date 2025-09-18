# HeapTupleHeaderIsOnlyLocked

## Location
[src/backend/access/heap/heapam_visibility.c:1520-1565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_visibility.c#L1520-L1565)

## Overview
HeapTupleHeaderIsOnlyLocked determines whether a tuple is only locked (not updated) by examining infomask bits and, for MultiXacts, verifying that the updating transaction has not committed.

## Definition
bool HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple)

## Detailed Description
This function determines if a tuple header represents only a lock operation rather than an update. For simple cases where the locker is not a MultiXact, it can determine the status by examining infomask bits alone. However, when dealing with MultiXacts, it must verify that any updating transaction within the MultiXact has actually aborted.

The function follows a systematic checking process:
1. First checks for invalid or explicitly lock-only XMAX values
2. For non-MultiXact cases, uses infomask bits to determine lock vs update status  
3. For MultiXact cases, extracts the updating XID and checks its transaction status
4. Returns true only if there is no committed update, meaning the tuple is only locked

The function follows the same visibility rules established elsewhere in the heapam_visibility.c file.

## Parameters / Member Variables
- `tuple`: The heap tuple header to examine for lock-only status

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetRawXmax
  - [HeapTupleGetUpdateXid](HeapTupleGetUpdateXid.md)
  - TransactionIdIsValid
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - TransactionIdIsInProgress
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - HEAP_XMAX_INVALID (macro)
  - HEAP_XMAX_LOCK_ONLY (macro)
  - HEAP_XMAX_IS_MULTI (macro)
- Called from (representative examples):
  - [heap_get_latest_tid](../h/heap_get_latest_tid.md)
  - [heap_delete](../h/heap_delete.md)
  - [heap_lock_tuple](../h/heap_lock_tuple.md)
  - [heap_lock_updated_tuple_rec](../h/heap_lock_updated_tuple_rec.md)
  - [HeapTupleSatisfiesVacuumHorizon](HeapTupleSatisfiesVacuumHorizon.md)
  - [rewrite_heap_tuple](../r/rewrite_heap_tuple.md)

## Notes and Other Information
- The function handles both simple lock cases (using infomask bits) and complex MultiXact cases (requiring transaction status checks)
- For MultiXacts, the function must extract the updating XID since a MultiXact can contain both lock and update operations
- The function returns true if the updating transaction has aborted or crashed, meaning only the lock remains
- It includes an assertion to ensure that non-LOCKED_ONLY tuples have valid XMAX values
- The logic follows PostgreSQL's MVCC visibility rules for determining tuple lock status