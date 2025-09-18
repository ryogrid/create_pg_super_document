# MultiXactIdWait

## Location
[src/backend/access/heap/heapam.c:7751-7772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L7751-L7772)

## Overview
MultiXactIdWait provides a blocking interface to wait for conflicting members of a multixact to complete before proceeding.

## Definition


## Detailed Description
This function serves as a simple wrapper around Do_MultiXactIdWait with blocking behavior enabled. It sleeps until all conflicting transactions in the specified multixact have completed. The function is designed to be called in a loop by the caller, as the tuple's Xmax may change while waiting, requiring re-evaluation.

The function unconditionally waits (blocking mode) and returns the count of remaining active members, including any non-aborted subtransactions from the current transaction. This count helps callers determine if further action is needed after the wait completes.

## Parameters / Member Variables
- : The multixact ID to wait for
- : The lock status being requested to determine conflicts
- : Tuple header information mask for optimization
- : Relation for error context information
- : Tuple identifier for error context
- : Operation type for error context and logging
- : Output parameter for count of remaining active members (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [Do_MultiXactIdWait](../D/Do_MultiXactIdWait.md) (with nowait=false)
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)
  - [heap_lock_tuple](../h/heap_lock_tuple.md)
  - [heap_inplace_lock](../h/heap_inplace_lock.md)

## Notes and Other Information
This is a static helper function that provides the standard blocking wait interface for multixact synchronization. Unlike ConditionalMultiXactIdWait, this function will always block until conflicting transactions complete. It's commonly used in heap access methods when the caller can afford to wait and doesn't need to handle the case where locks are unavailable immediately. The function's return is void because it always succeeds in the blocking mode.