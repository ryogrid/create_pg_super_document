# KnownAssignedXidsRemoveTree

## Location
src/backend/storage/ipc/procarray.c: 5012 - 5033

## Overview
Removes a main transaction ID and all its associated subtransaction IDs from the KnownAssignedXids array in a single operation.

## Definition
static void KnownAssignedXidsRemoveTree(TransactionId xid, int nsubxids, TransactionId *subxids)

## Detailed Description
KnownAssignedXidsRemoveTree provides bulk removal functionality for transaction trees, handling both the main transaction and all its subtransactions in one operation. The function first removes the main transaction ID (if it's valid) and then iterates through the array of subtransaction IDs, removing each one individually. After all removals are complete, it opportunistically compresses the KnownAssignedXids array to improve performance and reduce fragmentation.

This function is typically called during transaction completion (commit or abort) when an entire transaction tree needs to be cleaned up from the standby server's known assigned transactions tracking.

## Parameters / Member Variables
- xid: The main transaction ID to remove (may be InvalidTransactionId)
- nsubxids: The number of subtransaction IDs in the subxids array
- subxids: Array of subtransaction IDs to remove

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid
  - KnownAssignedXidsRemove
  - KnownAssignedXidsCompress
  - KAX_TRANSACTION_END
- Called from (representative examples):
  - ProcArrayApplyXidAssignment
  - ExpireTreeKnownAssignedTransactionIds

## Notes and Other Information
- Caller must hold ProcArrayLock in exclusive mode
- Handles InvalidTransactionId gracefully by checking validity before removal
- Performs opportunistic array compression after removals to improve performance
- Used during transaction completion to clean up entire transaction trees
- The compression operation helps maintain array efficiency by removing invalid entries