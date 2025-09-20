# HeapTupleSatisfiesUpdate

## Location
[src/backend/access/heap/heapam_visibility.c:458-742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_visibility.c#L458-L742)

## Overview
HeapTupleSatisfiesUpdate determines the visibility status of a heap tuple for UPDATE operations, providing detailed result codes beyond simple visibility to handle complex transaction scenarios including multi-transaction locking and command-level visibility.

## Definition

```c
struct is used as an
 * output argument to return the xids of concurrent xacts that affected the
 * tuple.  snapshot->xmin is set to the tuple's xmin if that is another
 * transaction that's still in progress;
```
## Detailed Description
This function implements PostgreSQL's tuple visibility checking specifically for UPDATE operations. Unlike other visibility functions that return simple boolean results, HeapTupleSatisfiesUpdate returns detailed status codes that UPDATE operations need to determine appropriate action. The function handles complex scenarios including:

- Transaction isolation and command-level visibility using CommandId
- Multi-transaction (MultiXact) locking scenarios
- Self-modification detection within the same transaction
- Differentiation between deleted and updated tuples
- Hint bit optimization for future visibility checks
- Legacy tuple movement handling for pre-9.0 binary upgrades

The function examines both the tuple's insertion transaction (xmin) and modification transaction (xmax) to determine the appropriate visibility status, considering whether transactions are committed, aborted, or still in progress.

## Parameters / Member Variables
- : The heap tuple to check for visibility, containing tuple data and metadata
- : The current command ID to determine command-level visibility within the current transaction
- : The buffer containing the tuple, used for setting hint bits to optimize future visibility checks

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderXminInvalid  
  - HeapTupleHeaderGetXvac
  - HeapTupleHeaderGetRawXmin
  - HeapTupleHeaderGetRawXmax
  - HeapTupleHeaderGetCmin
  - HeapTupleHeaderGetCmax
  - [HeapTupleGetUpdateXid](HeapTupleGetUpdateXid.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - TransactionIdIsInProgress
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [MultiXactIdIsRunning](../M/MultiXactIdIsRunning.md)
  - [SetHintBits](../S/SetHintBits.md)
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)
  - [heap_lock_tuple](../h/heap_lock_tuple.md)
  - [heap_inplace_lock](../h/heap_inplace_lock.md)

## Notes and Other Information
The function returns one of six TM_Result values:
- TM_Invisible: Tuple didn't exist when scan started
- TM_Ok: Tuple is valid and visible for update
- TM_SelfModified: Tuple was updated by current transaction after scan started
- TM_Updated: Tuple was updated by a committed transaction
- TM_Deleted: Tuple was deleted by a committed transaction  
- TM_BeingModified: Tuple is being modified by another in-progress transaction

The function includes special handling for HEAP_MOVED_OFF and HEAP_MOVED_IN flags used in pre-9.0 binary upgrades. It also optimizes performance through hint bit setting using SetHintBits when transaction states are determined.