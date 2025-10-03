# HeapTupleSatisfiesVacuumHorizon

## Location
[src/backend/access/heap/heapam_visibility.c:1196-1428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_visibility.c#L1196-L1428)

## Overview
HeapTupleSatisfiesVacuumHorizon is the core work horse function that determines vacuum status of heap tuples, providing detailed transaction-based visibility checking and returning the specific transaction ID that determines when a tuple becomes truly dead.

## Definition

```c
HTSV_Result
HeapTupleSatisfiesVacuumHorizon(HeapTuple htup, Buffer buffer, TransactionId *dead_after)
```
## Detailed Description
This function serves as the foundation for vacuum operations by performing comprehensive tuple visibility analysis. Unlike higher-level vacuum functions, HeapTupleSatisfiesVacuumHorizon focuses on determining the base vacuum status while providing the critical *dead_after transaction ID that callers can use to perform horizon-specific comparisons.

Key functionality includes:

**Insertion Transaction Analysis:**
- Checks if the inserting transaction (xmin) has committed, is in progress, or aborted
- Handles special cases for HEAP_MOVED_OFF and HEAP_MOVED_IN (pre-9.0 upgrade compatibility)
- Returns appropriate in-progress states for current transaction scenarios

**Deletion Transaction Analysis:**
- Examines the deleting/updating transaction (xmax) status
- Distinguishes between locked-only operations and actual deletions/updates
- Handles multi-transaction (MultiXact) scenarios for complex locking
- Provides transaction ID for recently dead tuples via dead_after parameter

**Status Determination:**
The function returns detailed HTSV_Result values and sets *dead_after to enable fine-grained vacuum decisions by callers who can compare against different horizons.

## Parameters / Member Variables
- `htup`: The heap tuple to analyze for vacuum status, containing tuple data and transaction metadata
- `buffer`: The buffer containing the tuple, used for setting hint bits to optimize future visibility checks
- `*dead_after`: Output parameter that receives the transaction ID after which the tuple becomes dead (for HEAPTUPLE_RECENTLY_DEAD results)
## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderXminInvalid
  - HeapTupleHeaderGetXvac
  - HeapTupleHeaderGetRawXmin
  - HeapTupleHeaderGetRawXmax
  - [HeapTupleHeaderIsOnlyLocked](HeapTupleHeaderIsOnlyLocked.md)
  - HeapTupleHeaderGetUpdateXid
  - [HeapTupleGetUpdateXid](HeapTupleGetUpdateXid.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - [TransactionIdIsInProgress](../T/TransactionIdIsInProgress.md)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [MultiXactIdIsRunning](../M/MultiXactIdIsRunning.md)
  - [SetHintBits](../S/SetHintBits.md)
- Called from (representative examples):
  - [HeapTupleSatisfiesVacuum](HeapTupleSatisfiesVacuum.md)
  - [HeapTupleSatisfiesNonVacuumable](HeapTupleSatisfiesNonVacuumable.md)
  - [heap_prune_satisfies_vacuum](../h/heap_prune_satisfies_vacuum.md)

## Notes and Other Information
The function returns HTSV_Result values indicating specific vacuum states:
- HEAPTUPLE_LIVE: Tuple is visible and cannot be removed
- HEAPTUPLE_RECENTLY_DEAD: Tuple was deleted but might still be visible to some transactions (dead_after set)
- HEAPTUPLE_DEAD: Tuple is not visible to any transaction and can be safely removed
- HEAPTUPLE_INSERT_IN_PROGRESS: Tuple insertion is still in progress
- HEAPTUPLE_DELETE_IN_PROGRESS: Tuple deletion/update is still in progress

The dead_after parameter is crucial for vacuum optimization - [when](../w/when.md) set, it indicates the specific transaction ID that determined the tuple's death. Callers can compare this against various horizons (oldest running transaction, etc.) to make precise vacuum decisions.

The function includes comprehensive hint bit management to optimize future tuple visibility checks, carefully balancing performance with correctness. Special handling for MultiXact scenarios ensures proper behavior in complex locking situations where multiple transactions may have interacted with the same tuple.

## Simplified Source

```c
HTSV_Result
HeapTupleSatisfiesVacuumHorizon(HeapTuple htup, Buffer buffer, TransactionId *dead_after)
{
    HeapTupleHeader tuple = htup->t_data;

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);
    Assert(dead_after != NULL);

    *dead_after = InvalidTransactionId;

    // Check if inserting transaction committed
    if (!HeapTupleHeaderXminCommitted(tuple)) {
        if (HeapTupleHeaderXminInvalid(tuple))
            return HEAPTUPLE_DEAD;

        // Handle current transaction cases
        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple))) {
            if (tuple->t_infomask & HEAP_XMAX_INVALID)
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            // Check for delete in same transaction
            if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tuple)))
                return HEAPTUPLE_DELETE_IN_PROGRESS;
            return HEAPTUPLE_INSERT_IN_PROGRESS;
        }

        // Check if insertion still in progress
        if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
            return HEAPTUPLE_INSERT_IN_PROGRESS;

        // Check if insertion committed or aborted
        if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple))) {
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
        } else {
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return HEAPTUPLE_DEAD;
        }
    }

    // Inserter committed, now check deleting transaction
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return HEAPTUPLE_LIVE;

    // Handle locked-only tuples
    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
        // Tuple is only locked, not deleted
        return HEAPTUPLE_LIVE;
    }

    // Handle multi-transaction scenarios
    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        TransactionId xmax = HeapTupleGetUpdateXid(tuple);

        if (TransactionIdIsInProgress(xmax))
            return HEAPTUPLE_DELETE_IN_PROGRESS;
        else if (TransactionIdDidCommit(xmax)) {
            *dead_after = xmax;
            return HEAPTUPLE_RECENTLY_DEAD;
        }
        return HEAPTUPLE_LIVE;
    }

    // Check simple deletion transaction
    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED)) {
        if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
            return HEAPTUPLE_DELETE_IN_PROGRESS;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple))) {
            SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
        } else {
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return HEAPTUPLE_LIVE;
        }
    }

    // Deleter committed - return transaction ID for horizon comparison
    *dead_after = HeapTupleHeaderGetRawXmax(tuple);
    return HEAPTUPLE_RECENTLY_DEAD;
}
```