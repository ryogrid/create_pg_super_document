# HeapTupleSatisfiesDirty

## Location
[src/backend/access/heap/heapam_visibility.c:743-959](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_visibility.c#L743-L959)

## Overview
HeapTupleSatisfiesDirty determines if a heap tuple is visible including effects of open (in-progress) transactions, implementing PostgreSQL's SNAPSHOT_DIRTY visibility semantics by returning transaction IDs of concurrent transactions affecting the tuple.

## Definition

```c
static bool
HeapTupleSatisfiesDirty(HeapTuple htup, Snapshot snapshot,
						Buffer buffer)
```
## Detailed Description
This function implements the "dirty read" visibility semantics for PostgreSQL's SNAPSHOT_DIRTY snapshots. Unlike other visibility functions that only consider committed transactions, HeapTupleSatisfiesDirty includes the effects of transactions still in progress. This is essential for certain internal operations that need to see uncommitted changes.

The function serves dual purposes:
1. Returns a boolean indicating tuple visibility under dirty read semantics
2. Uses the snapshot parameter as an output mechanism to report concurrent transaction IDs

Key behaviors include:
- Setting snapshot->xmin to the inserting transaction ID if it's still in progress
- Setting snapshot->xmax to the updating/deleting transaction ID if it's still in progress  
- Handling speculative insertions by returning the speculative token
- Similar to HeapTupleSatisfiesSelf for current transaction and committed transactions
- Includes effects of other in-progress transactions unlike standard visibility checks

## Parameters / Member Variables
- `htup`: The heap tuple to check for visibility, containing tuple data and metadata
- `snapshot`: Input/output parameter used to return concurrent transaction IDs affecting the tuple (xmin, xmax, speculativeToken)
- `buffer`: The buffer containing the tuple, used for setting hint bits to optimize future visibility checks
## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderXminInvalid
  - HeapTupleHeaderGetXvac
  - HeapTupleHeaderGetRawXmin
  - HeapTupleHeaderGetRawXmax
  - HeapTupleHeaderIsSpeculative
  - HeapTupleHeaderGetSpeculativeToken
  - [HeapTupleGetUpdateXid](HeapTupleGetUpdateXid.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - [TransactionIdIsInProgress](../T/TransactionIdIsInProgress.md)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [SetHintBits](../S/SetHintBits.md)
- Called from (representative examples):
  - [HeapTupleSatisfiesVisibility](HeapTupleSatisfiesVisibility.md)

## Notes and Other Information
The function is static and primarily used internally by the visibility subsystem. It implements SNAPSHOT_DIRTY semantics which are used for specific internal operations that need to observe uncommitted changes from other transactions.

The snapshot parameter serves as both input and output - the function modifies snapshot->xmin, snapshot->xmax, and snapshot->speculativeToken to inform the caller about concurrent transactions affecting the tuple. This information is crucial for operations that need to track or wait for concurrent transactions.

Special handling is included for speculative insertions, where the inserting transaction might still back down without aborting the entire transaction. The speculative token is returned to allow proper coordination between concurrent operations.

## Simplified Source

```c
static bool HeapTupleSatisfiesDirty(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    HeapTupleHeader tuple = htup->t_data;

    // Initialize output parameters
    snapshot->xmin = snapshot->xmax = InvalidTransactionId;
    snapshot->speculativeToken = 0;

    // Step 1: Check if tuple insertion is committed
    if (!HeapTupleHeaderXminCommitted(tuple)) {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        // Handle pre-9.0 binary upgrade cases (HEAP_MOVED_OFF/IN)
        if (handle_moved_tuples(tuple, buffer))
            return process_moved_result();

        // Handle current transaction's insertion
        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple))) {
            return handle_current_transaction_insert(tuple, buffer);
        }

        // Handle in-progress insertion by another transaction
        if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple))) {
            // Return speculative token if this is a speculative insert
            if (HeapTupleHeaderIsSpeculative(tuple)) {
                snapshot->speculativeToken = HeapTupleHeaderGetSpeculativeToken(tuple);
            }
            snapshot->xmin = HeapTupleHeaderGetRawXmin(tuple);
            return true; // Visible even though inserter hasn't committed
        }

        // Check if inserting transaction committed
        if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple))) {
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
        } else {
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return false;
        }
    }

    // Step 2: Insertion is committed, now check deletion/update status
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return true; // Not deleted/updated

    if (tuple->t_infomask & HEAP_XMAX_COMMITTED) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            return true; // Just locked, not deleted
        return false; // Actually deleted/updated
    }

    // Handle multixact (multiple lockers/updaters)
    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        return handle_multixact_case(tuple, snapshot);
    }

    // Handle single transaction in xmax
    TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);

    if (TransactionIdIsCurrentTransactionId(xmax)) {
        return !HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) ? false : true;
    }

    if (TransactionIdIsInProgress(xmax)) {
        // Report in-progress deleting transaction
        if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
            snapshot->xmax = xmax;
        return true; // Visible until deleter commits
    }

    if (!TransactionIdDidCommit(xmax)) {
        // Deleting transaction aborted
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    // Deleting transaction committed
    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, xmax);
    return false; // Deleted by committed transaction
}
```