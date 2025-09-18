# test_lockmode_for_conflict

## Location
[src/backend/access/heap/heapam.c:5561-5651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L5561-L5651)

## Overview
test_lockmode_for_conflict determines whether the current transaction can acquire a desired lock or must wait/fail, given a hypothetical lock status held by another transaction on a tuple.

## Definition
```c
static TM_Result test_lockmode_for_conflict(MultiXactStatus status, TransactionId xid,
                                            LockTupleMode mode, HeapTuple tup,
                                            bool *needwait)
```

## Detailed Description
This static function serves as a subroutine for heap_lock_updated_tuple_rec, implementing conflict detection logic for tuple locking operations. It analyzes the relationship between an existing hypothetical lock (held by a given transaction ID with a specific MultiXactStatus) and a desired lock mode to determine the appropriate action.

The function follows PostgreSQL's standard transaction state checking order: current transaction detection, in-progress status, abort status, and finally commit status. For each transaction state, it applies different conflict resolution rules:

- Current transaction: Returns TM_SelfModified for rare cases of self-locking
- In-progress transaction: Uses DoLockModesConflict to determine if waiting is necessary
- Aborted transaction: Always allows proceeding (TM_Ok)
- Committed transaction: Distinguishes between locks (which disappear) and updates (which persist)

The "hypothetical" nature of the status parameter allows the function to work uniformly with both single transaction IDs and MultiXactId members.

## Parameters / Member Variables
- `status`: MultiXactStatus representing the hypothetical lock held by the other transaction
- `xid`: TransactionId of the transaction holding the hypothetical lock
- `mode`: LockTupleMode desired by the current transaction
- `tup`: HeapTuple being locked (used for update chain detection)
- `needwait`: Output parameter set to true if the current transaction must wait

## Dependencies
- Functions called/Symbols referenced:
  - [get_mxact_status_for_lock](../g/get_mxact_status_for_lock.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - TransactionIdIsInProgress
  - [TransactionIdDidAbort](../T/TransactionIdDidAbort.md)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [DoLockModesConflict](../D/DoLockModesConflict.md)
  - LOCKMODE_from_mxstatus
  - ISUPDATE_from_mxstatus
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
- Called from (representative examples):
  - [heap_lock_updated_tuple_rec](../h/heap_lock_updated_tuple_rec.md)

## Notes and Other Information
- This is a static function internal to heapam.c, specifically designed for tuple locking logic
- The function must check TransactionIdIsInProgress before TransactionIdDidAbort/Commit due to visibility rules
- Returns different TM_Result codes: TM_Ok (can proceed), TM_SelfModified (self-lock), TM_Updated/TM_Deleted (conflicts)
- Handles the critical distinction that transaction locks disappear when transactions end, but updates persist
- The 'hypothetical' status design allows unified handling of both simple TransactionId locks and MultiXactId member locks
- For committed updates, uses tuple's t_self vs t_ctid comparison to distinguish between updates and deletes
- Critical for maintaining proper concurrency control and preventing lock conflicts in tuple locking operations