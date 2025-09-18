# AutoVacuumRequestWork

## Location
[src/backend/postmaster/autovacuum.c:3245-3286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L3245-L3286)

## Overview
Submits a work item request to the autovacuum system for processing in the next autovacuum run on the current database.

## Definition


## Detailed Description
This function provides an interface for database operations to request specific autovacuum work items. It manages a shared memory pool of work items that autovacuum workers can process asynchronously. The function is designed to be called from regular database operations that need autovacuum assistance.

Key functionality includes:

1. **Shared Memory Management**: Uses AutovacuumLock to safely access the shared work item pool
2. **Work Item Allocation**: Searches for an unused work item slot in the shared memory array
3. **Request Registration**: Populates work item details including type, database, relation, and optional block number
4. **Atomic Operation**: Ensures thread-safe work item creation through proper locking

The function returns success/failure status, allowing callers to handle cases where the work item pool is full.

## Parameters / Member Variables
- : Type of autovacuum work item to request (e.g., AVW_BRINSummarizeRange)
- : OID of the target relation for the work item
- : Optional block number for block-specific operations (can be InvalidBlockNumber)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (AutovacuumLock, LW_EXCLUSIVE)
  - LWLockRelease
  - AutoVacuumShmem (shared memory structure)
  - NUM_WORKITEMS (maximum work items)
- Called from (representative examples):
  - [brininsert](../b/brininsert.md)

## Notes and Other Information
- Returns  if no unused work item slots are available (pool is full)
- Work items are processed asynchronously by autovacuum workers in the same database
- Uses exclusive locking to prevent race conditions during work item allocation
- Each work item is marked as 'used' but not 'active' initially - activation occurs when processed
- The function automatically sets the current database ID for the work item
- Block numbers are optional and may be InvalidBlockNumber for relation-wide operations