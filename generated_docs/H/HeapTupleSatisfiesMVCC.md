# HeapTupleSatisfiesMVCC

## Location
[src/backend/access/heap/heapam_visibility.c:960-1161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_visibility.c#L960-L1161)

## Overview
HeapTupleSatisfiesMVCC determines if a heap tuple is visible according to MVCC (Multi-Version Concurrency Control) semantics for a given snapshot, implementing PostgreSQL's standard transaction isolation by checking tuple visibility against snapshot transaction boundaries.

## Definition

```c
static bool
HeapTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot,
					   Buffer buffer)
```
## Detailed Description
This function is the core implementation of PostgreSQL's MVCC visibility checking. It determines whether a tuple should be visible to a query operating under a specific snapshot. The function implements the fundamental MVCC rule: a tuple is visible if it was inserted by a transaction that committed before the snapshot was taken and has not been deleted by a transaction that committed before the snapshot was taken.

Key design principles:
- Avoids updating hint bits for transactions still running according to the snapshot, even if they're actually committed/aborted, to reduce contention
- Uses XidInMVCCSnapshot to check if transactions are visible in the given snapshot
- Handles command-level visibility within the current transaction using curcid
- Supports frozen transaction IDs for very old committed transactions
- Optimizes performance by deferring hint bit updates to reduce shared data structure access

The function carefully handles various tuple states including locked-only tuples, multi-transaction scenarios, and special cases for the current transaction.

## Parameters / Member Variables
- `htup`: The heap tuple to check for visibility, containing tuple data and metadata
- `snapshot`: The MVCC snapshot defining which transactions are visible, including xmin/xmax bounds and current command ID
- `buffer`: The buffer containing the tuple, used for setting hint bits when appropriate for performance optimization
## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderXminInvalid
  - HeapTupleHeaderXminFrozen
  - HeapTupleHeaderGetXvac
  - HeapTupleHeaderGetRawXmin
  - HeapTupleHeaderGetRawXmax
  - [HeapTupleHeaderGetCmin](HeapTupleHeaderGetCmin.md)
  - [HeapTupleHeaderGetCmax](HeapTupleHeaderGetCmax.md)
  - [HeapTupleGetUpdateXid](HeapTupleGetUpdateXid.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [XidInMVCCSnapshot](../X/XidInMVCCSnapshot.md)
  - [SetHintBits](../S/SetHintBits.md)
- Called from (representative examples):
  - [HeapTupleSatisfiesVisibility](HeapTupleSatisfiesVisibility.md)

## Notes and Other Information
The function is static and represents the standard MVCC visibility semantics used by most PostgreSQL queries. It implements careful optimization strategies to minimize contention on shared data structures like ProcArrayLock by avoiding premature hint bit updates.

The function handles legacy HEAP_MOVED_OFF and HEAP_MOVED_IN cases for pre-9.0 binary upgrade compatibility. For current transactions, it uses command ID comparison to implement statement-level read consistency within a transaction.

A critical optimization is that hint bits are only updated when transactions are definitively known to be committed/aborted according to the snapshot, avoiding the overhead of checking actual transaction status when it wouldn't change the visibility result. This design choice prioritizes reducing lock contention over immediate hint bit accuracy.

## Simplified Source

```c
static bool HeapTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    HeapTupleHeader tuple = htup->t_data;

    // Step 1: Check if tuple insertion (xmin) is committed
    if (!HeapTupleHeaderXminCommitted(tuple)) {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        // Handle pre-9.0 binary upgrade cases (HEAP_MOVED_OFF/IN)
        if (handle_moved_cases(tuple, snapshot, buffer))
            return process_moved_result();

        // Handle current transaction's insertion
        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple))) {
            // Check command-level visibility
            if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
                return false; // Inserted after scan started

            return handle_current_transaction_xmax(tuple, snapshot, buffer);
        }

        // Check if inserting transaction is visible in snapshot
        if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
            return false; // Still in progress according to snapshot

        // Check actual transaction status and set hint bits
        if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple))) {
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
        } else {
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return false;
        }
    }
    else {
        // xmin is marked committed, but check snapshot visibility
        if (!HeapTupleHeaderXminFrozen(tuple) &&
            XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
            return false; // Treat as still in progress
    }

    // Step 2: Insertion is visible, now check deletion/update (xmax)
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return true; // Not deleted

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
        return true; // Just locked, not deleted

    // Handle MultiXact (multiple concurrent operations)
    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        TransactionId xmax = HeapTupleGetUpdateXid(tuple);
        return handle_multixact_visibility(xmax, tuple, snapshot);
    }

    // Handle single deleting transaction
    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED)) {
        // Check if current transaction deleted this tuple
        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple))) {
            if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
                return true; // Deleted after scan started
            else
                return false; // Deleted before scan started
        }

        // Check if deleting transaction is visible in snapshot
        if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
            return true; // Deleter still in progress

        // Check actual transaction status
        if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple))) {
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return true; // Deleting transaction aborted
        }

        SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
    }
    else {
        // xmax is marked committed, but check snapshot visibility
        if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
            return true; // Treat deleter as still in progress
    }

    // Deleting transaction committed and is visible - tuple is deleted
    return false;
}
```