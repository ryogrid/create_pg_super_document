# ReadMultiXactCounts

## Location
src/backend/access/transam/multixact.c: 2918 - 2969

## Overview
Determines the current counts of multixacts and multixact members in the system by reading shared memory state.

## Definition


## Detailed Description
This function reads the current state from MultiXactState shared memory to calculate how many multixacts and multixact members currently exist in the system. It acquires the MultiXactGenLock in shared mode to safely read the next and oldest offsets and multixact IDs. The function can only provide accurate counts if the oldest offset is known; if this information is unavailable, it returns false.

The counts are calculated by taking the difference between the next and oldest values for both multixacts and member offsets, providing a snapshot of current system usage.

## Parameters / Member Variables
- : Pointer to store the count of current multixacts
- : Pointer to store the count of current multixact members

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease
  - MultiXactGenLock
  - MultiXactState (shared memory structure)
  - LW_SHARED
- Called from (representative examples):
  - MultiXactMemberFreezeThreshold (src/backend/access/transam/multixact.c:2979)

## Notes and Other Information
- Returns false if unable to determine counts (when oldestOffsetKnown is false)
- Returns true and sets output parameters when successful
- Uses shared lock on MultiXactGenLock for thread safety
- Calculates counts as simple differences between next and oldest values
- Critical for determining freeze thresholds and system resource usage
- Function is located at src/backend/access/transam/multixact.c:2918-2969