# KnownAssignedXidsRemovePreceding

## Location
src/backend/storage/ipc/procarray.c: 5034 - 5111

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
  - ProcArrayStruct
  - TransactionIdIsValid
  - elog (with DEBUG4 level)
  - TransactionIdFollowsOrEquals
  - StandbyTransactionIdIsPrepared
  - KnownAssignedXidsCompress
  - KAX_PRUNE
- Called from (representative examples):
  - ExpireAllKnownAssignedTransactionIds
  - ExpireOldKnownAssignedTransactionIds

## Notes and Other Information
- Caller must hold ProcArrayLock in exclusive mode
- If removeXid is invalid, clears the entire KnownAssignedXids array
- Preserves prepared transactions even if they precede the cutoff point
- Takes advantage of array sorting to stop scanning when reaching the first transaction to preserve
- Includes DEBUG4 logging for troubleshooting cleanup operations
- Automatically advances tail pointer over invalid entries for performance optimization
- Performs opportunistic compression after cleanup to maintain array efficiency