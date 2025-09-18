# ConditionalMultiXactIdWait

## Location
src/backend/access/heap/heapam.c: 7773 - 7786

## Overview
ConditionalMultiXactIdWait provides a non-blocking interface to conditionally wait for conflicting members of a multixact, returning immediately if locks cannot be acquired.

## Definition


## Detailed Description
This function serves as a wrapper around Do_MultiXactIdWait with non-blocking behavior enabled. Unlike MultiXactIdWait, it will not block if conflicting transactions are still active. Instead, it returns false to indicate that some transactions might still be running and the operation could not proceed immediately.

The function attempts to acquire locks on all conflicting multixact members, but if any lock cannot be obtained without blocking, it returns false. When it returns true, it means all conflicting transactions have completed and the caller can proceed. The remaining count provides information about how many members are still active.

## Parameters / Member Variables
- : The multixact ID to conditionally wait for
- : The lock status being requested to determine conflicts
- : Tuple header information mask for optimization
- : Relation for error context information
- : Output parameter for count of remaining active members (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [Do_MultiXactIdWait](../D/Do_MultiXactIdWait.md) (with nowait=true)
  - XLTW_None (for operation context)
- Called from (representative examples):
  - [heap_lock_tuple](../h/heap_lock_tuple.md) (multiple call sites)

## Notes and Other Information
This is a static helper function that provides the non-blocking wait interface for multixact synchronization. It's particularly useful in scenarios where the caller needs to handle lock unavailability gracefully, such as in lock acquisition with NOWAIT semantics or when implementing lock escalation strategies. The function returns true when the multixact is completely resolved, false when conflicts remain. Note that the remaining count should not be trusted when the function returns false, as documented in Do_MultiXactIdWait.