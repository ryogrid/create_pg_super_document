# heap_lock_updated_tuple_rec

## Location
src/backend/access/heap/heapam.c: 5652 - 5996

## Overview
heap_lock_updated_tuple_rec recursively locks a tuple and all its updated versions in an update chain, handling complex concurrency scenarios and MultiXactId management.

## Definition
```c
static TM_Result heap_lock_updated_tuple_rec(Relation rel, ItemPointer tid, TransactionId xid,
                                              LockTupleMode mode)
```

## Detailed Description
This static function serves as the recursive core of heap_lock_updated_tuple, implementing the complex logic needed to lock not just a specific tuple version, but all subsequent versions in its update chain. The function operates in a loop that follows tuple update chains by examining each tuple's t_ctid pointer.

For each tuple version encountered, the function:
- Fetches the tuple and validates it exists and wasn't vacuumed
- Handles visibility map pinning for performance optimization
- Checks for transaction conflicts using existing locks (both single TransactionId and MultiXactId)
- Computes new lock information using compute_new_xmax_infomask
- Updates the tuple's lock bits and logs the change via WAL
- Follows the update chain to the next version

The function implements sophisticated conflict detection by examining existing locks and using test_lockmode_for_conflict to determine whether to wait, proceed, or fail. It handles both MultiXactId scenarios (where multiple transactions have locks) and simple TransactionId cases.

A key optimization is detecting when the current transaction already holds a lock on a tuple version (TM_SelfModified), allowing it to skip redundant locking operations.

## Parameters / Member Variables
- `rel`: Relation containing the tuple to be locked
- `tid`: ItemPointer to the starting tuple in the update chain
- `xid`: TransactionId of the transaction requesting the lock
- `mode`: LockTupleMode specifying the strength of lock desired

## Dependencies
- Functions called/Symbols referenced:
  - [heap_fetch](heap_fetch.md)
  - [test_lockmode_for_conflict](../t/test_lockmode_for_conflict.md)
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - [XactLockTableWait](../X/XactLockTableWait.md)
  - [visibilitymap_pin](../v/visibilitymap_pin.md)
  - [visibilitymap_clear](../v/visibilitymap_clear.md)
  - [compute_infobits](../c/compute_infobits.md)
  - HeapTupleHeaderGetXmin, HeapTupleHeaderGetRawXmax, HeapTupleHeaderSetXmax
  - HeapTupleHeaderGetUpdateXid, HeapTupleHeaderIndicatesMovedPartitions
  - Various WAL logging functions (XLogBeginInsert, XLogRegisterBuffer, etc.)
- Called from (representative examples):
  - [heap_lock_updated_tuple](heap_lock_updated_tuple.md)

## Notes and Other Information
- This is a static function internal to heapam.c, implementing recursive tuple chain locking
- Uses an infinite loop with explicit termination conditions rather than traditional recursion to avoid stack overflow
- Handles complex visibility map management to maintain performance while ensuring consistency
- Implements proper WAL logging for crash recovery with xl_heap_lock_updated records
- The function can restart its processing (goto l4) when it needs to wait for other transactions
- Manages buffer locking carefully to avoid deadlocks while maintaining consistency
- Terminates recursion when reaching the end of update chain (invalid XMAX, moved partitions, or only-locked tuples)
- Critical for maintaining proper tuple locking semantics in PostgreSQL's MVCC system
- Includes extensive error handling for scenarios like aborted transactions and vacuumed tuples
- Performance-optimized with visibility map integration to minimize unnecessary I/O operations