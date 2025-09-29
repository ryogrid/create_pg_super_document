# HeapTupleSatisfiesSelf

## Location
[src/backend/access/heap/heapam_visibility.c:170-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_visibility.c#L170-L339)

## Overview
Determines if a heap tuple is visible to the current transaction under the "self" visibility rule, which means the tuple is visible if it was created by the current transaction and not deleted by it.

## Definition

```c
static bool
HeapTupleSatisfiesSelf(HeapTuple htup, Snapshot snapshot, Buffer buffer)
```
## Detailed Description
HeapTupleSatisfiesSelf implements the "self" visibility semantics for PostgreSQL's MVCC system. This function determines whether a tuple should be visible to the current transaction by examining the transaction IDs in the tuple header and comparing them with the current transaction.

The visibility logic follows these key principles:
- A tuple is visible if it was inserted by the current transaction (Xmin matches current txn)
- A tuple is not visible if it was deleted by the current transaction (Xmax matches current txn)
- A tuple is visible if it was committed by another transaction and not deleted by a committed transaction
- Special handling exists for pre-9.0 VACUUM FULL operations (HEAP_MOVED_OFF/HEAP_MOVED_IN flags)
- Multixact handling for cases where multiple transactions have locked/updated the tuple

The function also performs hint bit optimization by calling SetHintBits to cache transaction commit/abort status for future visibility checks.

## Parameters / Member Variables
- `htup`: The heap tuple to check for visibility
- `snapshot`: Snapshot context (not used in self-visibility but required for interface consistency)
- `buffer`: Buffer containing the tuple, used for hint bit updates

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderXminInvalid
  - HeapTupleHeaderGetXvac
  - HeapTupleHeaderGetRawXmin
  - HeapTupleHeaderGetRawXmax
  - [HeapTupleGetUpdateXid](HeapTupleGetUpdateXid.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - [TransactionIdIsInProgress](../T/TransactionIdIsInProgress.md)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [SetHintBits](../S/SetHintBits.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
- Called from (representative examples):
  - [HeapTupleSatisfiesVisibility](HeapTupleSatisfiesVisibility.md)

## Notes and Other Information
- This function is part of PostgreSQL's pluggable tuple visibility system
- Used primarily for READ COMMITTED isolation level and similar contexts
- The function handles complex multixact scenarios where tuples may be locked by multiple transactions
- Legacy support exists for pre-9.0 database upgrades via HEAP_MOVED_OFF/HEAP_MOVED_IN handling
- Performance is optimized through aggressive hint bit setting to avoid repeated transaction status lookups
- The function assumes the input tuple is valid and performs assertion checks on tuple consistency

## Simplified Source

```c
static bool
HeapTupleSatisfiesSelf(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    HeapTupleHeader tuple = htup->t_data;

    // Basic validation
    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    // Check if inserting transaction (Xmin) is committed
    if (!HeapTupleHeaderXminCommitted(tuple))
    {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        // Handle legacy pre-9.0 VACUUM FULL operations
        if (tuple->t_infomask & HEAP_MOVED_OFF)
        {
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);
            if (TransactionIdIsCurrentTransactionId(xvac))
                return false;
            // Check vacuum transaction status and set hint bits
            if (!TransactionIdIsInProgress(xvac))
            {
                if (TransactionIdDidCommit(xvac))
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                else
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
                return false;
            }
        }
        else if (tuple->t_infomask & HEAP_MOVED_IN)
        {
            // Similar handling for MOVED_IN tuples
            TransactionId xvac = HeapTupleHeaderGetXvac(tuple);
            if (!TransactionIdIsCurrentTransactionId(xvac))
            {
                // Check and set appropriate hint bits based on vacuum transaction status
                if (TransactionIdIsInProgress(xvac))
                    return false;
                if (TransactionIdDidCommit(xvac))
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
                else
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            }
        }
        // Current transaction inserted this tuple
        else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
        {
            // Tuple inserted by current transaction - check if it's been deleted
            if (tuple->t_infomask & HEAP_XMAX_INVALID)
                return true;  // Not deleted

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
                return true;  // Only locked, not deleted

            // Handle multixact scenarios
            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
            {
                TransactionId xmax = HeapTupleGetUpdateXid(tuple);
                return !TransactionIdIsCurrentTransactionId(xmax);
            }

            // Check if current transaction deleted it
            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
            {
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
                return true;
            }
            return false;  // Current transaction deleted it
        }
        // Check inserting transaction status
        else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
            return false;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
        else
        {
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return false;
        }
    }

    // Inserting transaction committed - check deletion status
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return true;  // Not deleted

    if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;  // Only locked
        return false;  // Deleted by committed transaction
    }

    // Handle multixact deletion
    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;

        TransactionId xmax = HeapTupleGetUpdateXid(tuple);
        if (TransactionIdIsCurrentTransactionId(xmax))
            return false;  // Current transaction deleted it
        if (TransactionIdIsInProgress(xmax))
            return true;   // Deleting transaction still running
        return !TransactionIdDidCommit(xmax);  // Visible if deleting transaction aborted
    }

    // Simple deletion case
    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
    {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true;
        return false;  // Current transaction deleted it
    }

    if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
        return true;  // Deleting transaction still running

    if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
    {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;  // Deleting transaction aborted
    }

    // Deleting transaction committed
    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
    {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;  // Was only locked
    }

    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
    return false;  // Tuple was deleted
}
```