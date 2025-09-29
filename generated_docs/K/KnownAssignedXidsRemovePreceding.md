# KnownAssignedXidsRemovePreceding

## Location
[src/backend/storage/ipc/procarray.c:5034-5111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L5034-L5111)

## Overview
Prunes the KnownAssignedXids array by removing all transaction IDs that precede a specified cutoff point, while preserving prepared transactions.

## Definition
static void KnownAssignedXidsRemovePreceding(TransactionId removeXid)

## Detailed Description
KnownAssignedXidsRemovePreceding performs bulk cleanup of the KnownAssignedXids array by removing all transaction IDs that are older than the specified cutoff transaction ID. The function takes advantage of the array's sorted nature to efficiently scan from the tail until it reaches a transaction ID that should be preserved.

The function has special handling for two cases: if removeXid is invalid, it clears the entire array; otherwise, it preserves prepared transactions even if they precede the cutoff point. After marking entries as invalid, the function updates the tail pointer to skip over invalid entries and performs opportunistic compression to maintain array efficiency.

This operation is typically used during transaction cleanup phases to remove old transaction IDs that are no longer needed for visibility determination.

## Parameters / Member Variables
- removeXid: The cutoff transaction ID; all preceding valid transactions will be removed (unless they are prepared transactions)

## Dependencies
- Functions called/Symbols referenced:
  - [ProcArrayStruct](../P/ProcArrayStruct.md)
  - TransactionIdIsValid
  - elog (with DEBUG4 level)
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md)
  - [StandbyTransactionIdIsPrepared](../S/StandbyTransactionIdIsPrepared.md)
  - [KnownAssignedXidsCompress](KnownAssignedXidsCompress.md)
  - KAX_PRUNE
- Called from (representative examples):
  - [ExpireAllKnownAssignedTransactionIds](../E/ExpireAllKnownAssignedTransactionIds.md)
  - [ExpireOldKnownAssignedTransactionIds](../E/ExpireOldKnownAssignedTransactionIds.md)

## Notes and Other Information
- Caller must hold ProcArrayLock in exclusive mode
- If removeXid is invalid, clears the entire KnownAssignedXids array
- Preserves prepared transactions even if they precede the cutoff point
- Takes advantage of array sorting to stop scanning when reaching the first transaction to preserve
- Includes DEBUG4 logging for troubleshooting cleanup operations
- Automatically advances tail pointer over invalid entries for performance optimization
- Performs opportunistic compression after cleanup to maintain array efficiency

## Simplified Source

```c
// Remove all transaction IDs older than removeXid from KnownAssignedXids array
static void KnownAssignedXidsRemovePreceding(TransactionId removeXid)
{
    ProcArrayStruct *pArray = procArray;
    int count = 0;
    int head, tail, i;

    // Special case: clear entire array if removeXid is invalid
    if (!TransactionIdIsValid(removeXid)) {
        elog(DEBUG4, "removing all KnownAssignedXids");
        pArray->numKnownAssignedXids = 0;
        pArray->headKnownAssignedXids = pArray->tailKnownAssignedXids = 0;
        return;
    }

    elog(DEBUG4, "prune KnownAssignedXids to %u", removeXid);

    // Mark old entries as invalid (array is sorted, so we can stop early)
    tail = pArray->tailKnownAssignedXids;
    head = pArray->headKnownAssignedXids;

    for (i = tail; i < head; i++) {
        if (KnownAssignedXidsValid[i]) {
            TransactionId knownXid = KnownAssignedXids[i];

            // Stop when we reach newer transactions
            if (TransactionIdFollowsOrEquals(knownXid, removeXid))
                break;

            // Mark as invalid unless it's a prepared transaction
            if (!StandbyTransactionIdIsPrepared(knownXid)) {
                KnownAssignedXidsValid[i] = false;
                count++;
            }
        }
    }

    // Update count
    pArray->numKnownAssignedXids -= count;
    Assert(pArray->numKnownAssignedXids >= 0);

    // Advance tail pointer past invalid entries
    for (i = tail; i < head; i++) {
        if (KnownAssignedXidsValid[i])
            break;
    }

    if (i >= head) {
        // Array is empty, reset pointers
        pArray->headKnownAssignedXids = 0;
        pArray->tailKnownAssignedXids = 0;
    } else {
        pArray->tailKnownAssignedXids = i;
    }

    // Compress array to remove gaps
    KnownAssignedXidsCompress(KAX_PRUNE, true);
}
```