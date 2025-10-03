# ReadMultiXactCounts

## Location
[src/backend/access/transam/multixact.c:2918-2969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2918-L2969)

## Overview
Determines the current counts of multixacts and multixact members in the system by reading shared memory state.

## Definition

```c
static bool
ReadMultiXactCounts(uint32 *multixacts, MultiXactOffset *members)
```
## Detailed Description
This function reads the current state from MultiXactState shared memory to calculate how many multixacts and multixact members currently exist in the system. It acquires the MultiXactGenLock in shared mode to safely read the next and oldest offsets and multixact IDs. The function can only provide accurate counts if the oldest offset is known; if this information is unavailable, it returns false.

The counts are calculated by taking the difference between the next and oldest values for both multixacts and member offsets, providing a snapshot of current system usage.

## Parameters / Member Variables
- `*multixacts`: Pointer to store the count of current multixacts
- `*members`: Pointer to store the count of current multixact members
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - MultiXactGenLock
  - MultiXactState (shared memory structure)
  - LW_SHARED
- Called from (representative examples):
  - [MultiXactMemberFreezeThreshold](../M/MultiXactMemberFreezeThreshold.md) (src/backend/access/transam/multixact.c:2979)

## Notes and Other Information
- Returns false if unable to determine counts (when oldestOffsetKnown is false)
- Returns true and sets output parameters when successful
- Uses shared lock on MultiXactGenLock for thread safety
- Calculates counts as simple differences between next and oldest values
- Critical for determining freeze thresholds and system resource usage
- Function is located at src/backend/access/transam/multixact.c:2918-2969

## Simplified Source

```c
static bool
ReadMultiXactCounts(uint32 *multixacts, MultiXactOffset *members)
{
    MultiXactOffset nextOffset;
    MultiXactOffset oldestOffset;
    MultiXactId oldestMultiXactId;
    MultiXactId nextMultiXactId;
    bool oldestOffsetKnown;

    // Read current state from shared memory under lock
    LWLockAcquire(MultiXactGenLock, LW_SHARED);
    nextOffset = MultiXactState->nextOffset;
    oldestMultiXactId = MultiXactState->oldestMultiXactId;
    nextMultiXactId = MultiXactState->nextMXact;
    oldestOffset = MultiXactState->oldestOffset;
    oldestOffsetKnown = MultiXactState->oldestOffsetKnown;
    LWLockRelease(MultiXactGenLock);

    // Can't calculate without known oldest offset
    if (!oldestOffsetKnown)
        return false;

    // Calculate counts as differences between next and oldest values
    *members = nextOffset - oldestOffset;
    *multixacts = nextMultiXactId - oldestMultiXactId;
    return true;
}
```