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
  - [HeapTupleHeaderGetCmin](HeapTupleHeaderGetCmin.md)
  - [HeapTupleHeaderGetCmax](HeapTupleHeaderGetCmax.md)
  - [HeapTupleGetUpdateXid](HeapTupleGetUpdateXid.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - [TransactionIdIsInProgress](../T/TransactionIdIsInProgress.md)
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

## Simplified Source

```c
// Simplified version of HeapTupleSatisfiesUpdate
TM_Result HeapTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid, Buffer buffer) {
    HeapTupleHeader tuple = htup->t_data;

    // Check if inserting transaction (xmin) is committed
    if (!HeapTupleHeaderXminCommitted(tuple)) {
        if (HeapTupleHeaderXminInvalid(tuple))
            return TM_Invisible;

        // Handle legacy pre-9.0 tuple movement
        if (tuple->t_infomask & HEAP_MOVED_OFF) {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);
            if (TransactionIdIsCurrentTransactionId(xvac))
                return TM_Invisible;
            // Check if vacuum transaction committed/aborted
            if (!TransactionIdIsInProgress(xvac)) {
                if (TransactionIdDidCommit(xvac)) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return TM_Invisible;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
            }
        }
        else if (tuple->t_infomask & HEAP_MOVED_IN) {
            // Similar handling for HEAP_MOVED_IN case
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);
            if (!TransactionIdIsCurrentTransactionId(xvac)) {
                if (TransactionIdIsInProgress(xvac))
                    return TM_Invisible;
                if (!TransactionIdDidCommit(xvac)) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return TM_Invisible;
                }
            }
        }
        // Check if tuple was inserted by current transaction
        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple))) {
            if (HeapTupleHeaderGetCmin(tuple) >= curcid)
                return TM_Invisible; // Inserted after scan started

            // Handle various xmax states for self-inserted tuples
            if (tuple->t_infomask & HEAP_XMAX_INVALID)
                return TM_Ok;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
                // Check if tuple is locked by other transactions
                TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);
                if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
                    return MultiXactIdIsRunning(xmax, true) ? TM_BeingModified : TM_Ok;
                }
                return TransactionIdIsInProgress(xmax) ? TM_BeingModified : TM_Ok;
            }

            // Handle update/delete by current transaction
            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
                TransactionId xmax = HeapTupleGetUpdateXid(tuple);
                if (!TransactionIdIsCurrentTransactionId(xmax)) {
                    return MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false)
                           ? TM_BeingModified : TM_Ok;
                }
                return (HeapTupleHeaderGetCmax(tuple) >= curcid) ? TM_SelfModified : TM_Invisible;
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple))) {
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
                return TM_Ok;
            }

            return (HeapTupleHeaderGetCmax(tuple) >= curcid) ? TM_SelfModified : TM_Invisible;
        }
        // Check status of inserting transaction
        else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
            return TM_Invisible;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
        else {
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return TM_Invisible;
        }
    }

    // Inserting transaction has committed - check modification status
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return TM_Ok;

    if (tuple->t_infomask & HEAP_XMAX_COMMITTED) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return TM_Ok;
        // Check if tuple was updated or deleted
        return ItemPointerEquals(&htup->t_self, &tuple->t_ctid) ? TM_Deleted : TM_Updated;
    }

    // Handle MultiXact cases
    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        if (HEAP_LOCKED_UPGRADED(tuple->t_infomask))
            return TM_Ok;

        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
            if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true))
                return TM_BeingModified;
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return TM_Ok;
        }

        TransactionId xmax = HeapTupleGetUpdateXid(tuple);
        if (TransactionIdIsCurrentTransactionId(xmax)) {
            return (HeapTupleHeaderGetCmax(tuple) >= curcid) ? TM_SelfModified : TM_Invisible;
        }

        if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
            return TM_BeingModified;

        if (TransactionIdDidCommit(xmax)) {
            return ItemPointerEquals(&htup->t_self, &tuple->t_ctid) ? TM_Deleted : TM_Updated;
        }

        // Update aborted, check for remaining lockers
        if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false)) {
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return TM_Ok;
        }
        return TM_BeingModified;
    }

    // Handle single transaction xmax
    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple))) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return TM_BeingModified;
        return (HeapTupleHeaderGetCmax(tuple) >= curcid) ? TM_SelfModified : TM_Invisible;
    }

    if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
        return TM_BeingModified;

    if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple))) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return TM_Ok;
    }

    // Xmax transaction committed
    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return TM_Ok;
    }

    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
    return ItemPointerEquals(&htup->t_self, &tuple->t_ctid) ? TM_Deleted : TM_Updated;
}
```

Key simplifications made:
- Consolidated similar transaction state checking patterns
- Removed redundant assertions and detailed comments
- Simplified complex nested conditions into clearer logic flow
- Grouped related MultiXact handling cases together
- Abstracted hint bit setting operations with descriptive comments
- Preserved all essential visibility logic and return codes
- Maintained the core algorithm structure while reducing verbosity by ~40%