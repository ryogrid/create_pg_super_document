# MultiXactIdSetOldestMember

## Location
[src/backend/access/transam/multixact.c:672-728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L672-L728)

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


## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdIsValid
  - [LWLockAcquire](../L/LWLockAcquire.md) (with LW_SHARED)
  - [LWLockRelease](../L/LWLockRelease.md)
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

## Simplified Source

```c
void MultiXactIdSetOldestMember(void) {
    // Only set if not already initialized for this transaction
    if (!MultiXactIdIsValid(OldestMemberMXactId[MyProcNumber])) {

        // Acquire lock to safely read nextMXact
        LWLockAcquire(MultiXactGenLock, LW_SHARED);

        // Get next MultiXactId to be assigned
        MultiXactId nextMXact = MultiXactState->nextMXact;

        // Handle wraparound case - ensure value is at least FirstMultiXactId
        if (nextMXact < FirstMultiXactId)
            nextMXact = FirstMultiXactId;

        // Store the oldest member ID for this process
        OldestMemberMXactId[MyProcNumber] = nextMXact;

        LWLockRelease(MultiXactGenLock);

        debug_elog4(DEBUG2, "MultiXact: setting OldestMember[%d] = %u",
                    MyProcNumber, nextMXact);
    }
}
```