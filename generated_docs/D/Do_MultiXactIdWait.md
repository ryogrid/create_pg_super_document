# Do_MultiXactIdWait

## Location
src/backend/access/heap/heapam.c: 7673 - 7750

## Overview
Do_MultiXactIdWait is the core implementation function that waits for conflicting members of a multixact to complete, supporting both blocking and non-blocking wait modes.

## Definition


## Detailed Description
This function implements the actual waiting logic for multixact conflicts by:

1. Retrieving all members of the specified multixact ID
2. Iterating through each member to identify conflicts with the requested status
3. Skipping members that belong to the current backend (to avoid deadlock)
4. Using XactLockTableWait or ConditionalXactLockTableWait to wait for conflicting transactions
5. Tracking the number of remaining active members

The function handles pre-upgrade tuples as a special case and supports both conditional (nowait) and unconditional waiting modes. It ensures that by completion, all conflicting transactions from other backends have finished, though the caller may need to iterate if the tuple's Xmax has changed during the wait.

## Parameters / Member Variables
- : The multixact ID whose members need to be waited for
- : The lock status being requested, used to determine conflicts
- : Tuple header information mask for optimization
- : If true, use conditional locking to avoid blocking
- : Relation for error context information
- : Tuple identifier for error context
- : Operation type for error context
- : Output parameter for count of remaining active members (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - HEAP_LOCKED_UPGRADED
  - GetMultiXactIdMembers
  - HEAP_XMAX_IS_LOCKED_ONLY
  - TransactionIdIsCurrentTransactionId
  - DoLockModesConflict
  - LOCKMODE_from_mxstatus
  - TransactionIdIsInProgress
  - ConditionalXactLockTableWait
  - XactLockTableWait
- Called from (representative examples):
  - MultiXactIdWait
  - ConditionalMultiXactIdWait

## Notes and Other Information
This is a static helper function that serves as the common implementation for both MultiXactIdWait and ConditionalMultiXactIdWait. It's critical for PostgreSQL's tuple locking mechanism, ensuring proper synchronization when multiple transactions compete for tuple access. The function carefully avoids waiting on transactions from the same backend to prevent assertion failures and deadlocks. The remaining count is unreliable when the function returns false (in nowait mode).