# NumLWLocksForNamedTranches

## Location
[src/backend/storage/lmgr/lwlock.c:408-422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L408-L422)

## Overview
Computes the total number of lightweight locks required by all registered named tranches for allocation in the main LWLock array.

## Definition

```c
static int
NumLWLocksForNamedTranches(void)
```
## Detailed Description
This function iterates through all registered named LWLock tranche requests and calculates the cumulative number of locks required. Named tranches allow extensions and different PostgreSQL subsystems to register their lightweight lock requirements during system initialization. The function provides the total count that needs to be allocated in the main LWLock array to accommodate all named tranche requests.

The function accesses global arrays that store the tranche registration information, summing up the num_lwlocks field from each registered tranche request.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - None (simple arithmetic computation)
- Global variables accessed:
  - NamedLWLockTrancheRequests (counter of registered tranches)
  - NamedLWLockTrancheRequestArray (array of tranche requests)
- Called from:
  - [LWLockShmemSize](../L/LWLockShmemSize.md) (src/backend/storage/lmgr/lwlock.c:430)
  - [InitializeLWLocks](../I/InitializeLWLocks.md) (src/backend/storage/lmgr/lwlock.c:495)

## Notes and Other Information
- Returns the total count of locks needed across all named tranches
- Used during shared memory sizing calculations and LWLock array initialization
- Part of the extensible LWLock tranche system that allows modules to register lock requirements
- The named tranche system enables clean separation between core PostgreSQL locks and extension-specific locks
- This count is added to the base LWLock requirements to determine the total LWLock array size
- Function is typically called during system startup before the main LWLock array is allocated

## Simplified Source

```c
// Simplified version of NumLWLocksForNamedTranches
static int NumLWLocksForNamedTranches(void) {
    int numLocks = 0;
    int i;

    // Sum up lock requirements from all named tranches
    for (i = 0; i < NamedLWLockTrancheRequests; i++) {
        numLocks += NamedLWLockTrancheRequestArray[i].num_lwlocks;
    }

    return numLocks;
}
```

Key simplifications made:
- Removed detailed comments for clarity
- Function is already simple, maintained the essential summation logic
- Focused on the core iteration and accumulation of lock counts