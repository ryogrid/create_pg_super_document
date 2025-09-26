# GetNamedLWLockTranche

## Location
[src/backend/storage/lmgr/lwlock.c:576-605](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L576-L605)

## Overview
Returns the base address of the first LWLock in a named tranche, allowing extensions to access their requested LWLock groups.

## Definition

```c
LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name)
```
## Detailed Description
GetNamedLWLockTranche provides extensions and other components with access to their named LWLock tranches by returning the base address of the first lock in the specified tranche. The function searches through the NamedLWLockTrancheRequestArray to find the tranche with the matching name and calculates its position in the MainLWLockArray.

Named tranches are allocated in the MainLWLockArray after all fixed locks (individual, buffer mapping, lock manager, and predicate lock manager locks). The function iterates through the tranche requests in order, accumulating the lock positions until it finds the requested tranche name.

Once the caller obtains the base address, they can access the full range of locks in their tranche by indexing from the returned pointer. This allows extensions that requested multiple LWLocks via RequestNamedLWLockTranche() to access all their locks systematically.

## Parameters / Member Variables
- : The name of the tranche to look up, as specified when the tranche was requested

## Dependencies
- Functions called/Symbols referenced:
  - strcmp: String comparison to match tranche names
  - elog: Error logging for unregistered tranche names
- Constants used:
  - NUM_FIXED_LWLOCKS: Starting position for named tranches in MainLWLockArray
- Global variables accessed:
  - MainLWLockArray: The main array of LWLocks
  - NamedLWLockTrancheRequests: Number of named tranche requests
  - NamedLWLockTrancheRequestArray: Array of tranche request information
- Called from:
  - Extensions and other components needing access to their named LWLock tranches

## Notes and Other Information
- The function performs a linear search through tranche requests, so tranche lookup performance depends on the number of named tranches
- If the requested tranche name is not found, the function calls elog(ERROR) which will abort the current transaction
- The returned pointer points to LWLockPadded structures, which include padding for cache line alignment
- Callers must know how many locks they requested in their tranche to avoid accessing beyond their allocated range
- The function assumes that named tranches are allocated contiguously after fixed locks in MainLWLockArray
- This is part of the extension API for LWLock management, allowing extensions to get typed access to their lock resources