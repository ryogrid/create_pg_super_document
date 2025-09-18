# MultiXactIdSetOldestMember

## Location
src/backend/access/transam/multixact.c: 672 - 728

## Overview
MultiXactIdSetOldestMember records the oldest MultiXactId that the current transaction could potentially be a member of, establishing a reference point for MultiXactId operations.

## Definition
void MultiXactIdSetOldestMember(void)

## Detailed Description
This function sets the OldestMemberMXactId for the current transaction to track the oldest MultiXactId that this transaction could be a member of. This value is set to the next-to-be-assigned MultiXactId at the time of the call.

The function is designed to be called before any operation that might require a MultiXactId (such as tuple locks, updates, or deletes), even if the operation ultimately uses a regular TransactionId instead. This preemptive approach is necessary because other concurrent transactions might add our transaction ID to a MultiXactId.

Key implementation details:
- Only sets the value if it hasn't been set already (checked via MultiXactIdIsValid)
- Acquires MultiXactGenLock in shared mode to ensure atomic reading of nextMXact
- Handles the wrapped-around state by ensuring the stored value is at least FirstMultiXactId
- The shared lock is sufficient to prevent others from advancing nextMXact during the operation

This mechanism is crucial for maintaining consistency in MultiXactId operations and preventing race conditions in concurrent transaction scenarios.

## Parameters / Member Variables
- No parameters (operates on global state)

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdIsValid
  - LWLockAcquire (with LW_SHARED)
  - LWLockRelease
  - debug_elog4
- Global variables accessed:
  - OldestMemberMXactId[MyProcNumber]
  - MultiXactState->nextMXact
  - FirstMultiXactId
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md) (src/backend/access/heap/heapam.c:2990)
  - [heap_update](../h/heap_update.md) (src/backend/access/heap/heapam.c:3409)
  - [heap_lock_tuple](../h/heap_lock_tuple.md) (src/backend/access/heap/heapam.c:5108)
  - [heap_lock_updated_tuple](../h/heap_lock_updated_tuple.md) (src/backend/access/heap/heapam.c:6016)

## Notes and Other Information
- Must be called before any operation that might create or participate in a MultiXactId
- Uses shared locking strategy - sufficient to prevent nextMXact advancement while allowing concurrent reads
- Handles MultiXactId wraparound by ensuring stored values are at least FirstMultiXactId
- Critical for preventing race conditions where OldestVisibleMXactId could be computed incorrectly
- Once set for a transaction, the value remains unchanged (idempotent operation)
- Essential for the proper functioning of MultiXactIdExpand and other MultiXactId operations