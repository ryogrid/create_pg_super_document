# ReorderBufferImmediateInvalidation

## Location
src/backend/replication/logical/reorderbuffer.c: 3134 - 3169

## Overview
Executes cache invalidation messages immediately outside the context of a decoded transaction, ensuring catalog cache consistency without requiring full transaction state setup.

## Definition
```c
void ReorderBufferImmediateInvalidation(ReorderBuffer *rb, uint32 ninvalidations, SharedInvalidationMessage *invalidations)
```

## Detailed Description
ReorderBufferImmediateInvalidation handles the execution of cache invalidation messages that need to be processed outside the normal transaction decoding context. This occurs in two main scenarios: xid-less commits and invalidations from transactions that are not of interest to the current decoding session.

The function employs a sophisticated transaction management strategy to ensure invalidations are processed correctly:

1. If already in a transaction context, it begins an internal subtransaction
2. It deliberately aborts the current transaction to force invalidations to execute outside a valid transaction state
3. This approach ensures that cache entries are simply marked as invalid without requiring catalog access
4. The subtransaction is then rolled back and released to clean up the transaction state

This design is advantageous because it avoids the need to set up the full state normally required for catalog access during invalidation processing.

## Parameters / Member Variables
- `rb`: The ReorderBuffer instance (currently not used in the function body)
- `ninvalidations`: Number of invalidation messages to process
- `invalidations`: Array of SharedInvalidationMessage structures containing the invalidation data

## Dependencies
- Functions called/Symbols referenced:
  - IsTransactionOrTransactionBlock
  - BeginInternalSubTransaction
  - AbortCurrentTransaction
  - LocalExecuteInvalidationMessage
  - RollbackAndReleaseCurrentSubTransaction
- Called from (representative examples):
  - xact_decode (in decode.c)
  - ReorderBufferAbort
  - ReorderBufferForget
  - ReorderBufferInvalidate

## Notes and Other Information
- Handles both xid-less commits and invalidations from uninteresting transactions
- Uses internal subtransactions to manage the execution context safely
- Forces invalidations to execute outside valid transaction state for efficiency
- Does not require full catalog access setup, making it more lightweight
- The ReorderBuffer parameter is accepted but not actively used in the current implementation
- Critical for maintaining catalog cache consistency across different invalidation scenarios