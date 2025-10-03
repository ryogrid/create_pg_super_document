# SetOffsetVacuumLimit

## Location
[src/backend/access/transam/multixact.c:2705-2831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2705-L2831)

## Overview
Determines how aggressively vacuum needs to run to prevent member wraparound by calculating the oldest member offset and installing limit information in MultiXactState.

## Definition

```c
static bool
SetOffsetVacuumLimit(bool is_startup)
```
## Detailed Description
This function determines the oldest MultiXact member offset and installs limit information in MultiXactState to prevent overrun of old data in the members SLRU area. It calculates whether emergency autovacuum is required based on the distance between the next offset and oldest offset. The function handles special cases where no multixacts exist and accounts for bugs in early PostgreSQL 9.3.X and 9.4.X releases where the oldest multixact might not actually exist on disk.

The function acquires locks to prevent concurrent truncation and reads shared memory state. It computes a stop limit by moving back to the start of the corresponding segment and leaving one segment before the wraparound point as a safety buffer.

## Parameters / Member Variables
- `is_startup`: Boolean indicating whether this is being called during startup (affects logging behavior)
## Dependencies
- Functions called/Symbols referenced:
  - [find_multixact_start](../f/find_multixact_start.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - ereport
  - [errmsg_internal](../e/errmsg_internal.md)
  - [errmsg](../e/errmsg.md)
  - MULTIXACT_MEMBERS_PER_PAGE
  - SLRU_PAGES_PER_SEGMENT
  - MULTIXACT_MEMBER_SAFE_THRESHOLD
- Called from (representative examples):
  - debug_elog6 (src/backend/access/transam/multixact.c:413)
  - [SetMultiXactIdLimit](SetMultiXactIdLimit.md) (src/backend/access/transam/multixact.c:2440)

## Notes and Other Information
- Returns true if emergency autovacuum is required, false otherwise
- Protects against member wraparound in the MultiXact SLRU
- Uses MultiXactTruncationLock and MultiXactGenLock for synchronization
- Handles edge cases where oldest multixact data might be missing due to historical bugs
- Logs diagnostic information about member offsets and wraparound protection status
- Function is located at src/backend/access/transam/multixact.c:2705-2831

## Simplified Source

```c
// Simplified version of SetOffsetVacuumLimit
static bool SetOffsetVacuumLimit(bool is_startup) {
    MultiXactId oldestMultiXactId, nextMXact;
    MultiXactOffset oldestOffset = 0;
    MultiXactOffset nextOffset;
    bool oldestOffsetKnown = false;
    MultiXactOffset offsetStopLimit = 0;

    // Prevent concurrent truncation during analysis
    LWLockAcquire(MultiXactTruncationLock, LW_SHARED);

    // Read current state from shared memory
    LWLockAcquire(MultiXactGenLock, LW_SHARED);
    oldestMultiXactId = MultiXactState->oldestMultiXactId;
    nextMXact = MultiXactState->nextMXact;
    nextOffset = MultiXactState->nextOffset;
    // ... read other shared state variables
    LWLockRelease(MultiXactGenLock);

    // Determine oldest multixact offset
    if (oldestMultiXactId == nextMXact) {
        // No existing multixacts - use next offset
        oldestOffset = nextOffset;
        oldestOffsetKnown = true;
    } else {
        // Find where oldest multixact's data is stored
        oldestOffsetKnown = find_multixact_start(oldestMultiXactId, &oldestOffset);

        if (oldestOffsetKnown) {
            // Log successful offset discovery
        } else {
            // Log warning about missing multixact data
        }
    }

    LWLockRelease(MultiXactTruncationLock);

    // Calculate wraparound protection limits if offset is known
    if (oldestOffsetKnown) {
        // Move back to segment boundary
        offsetStopLimit = oldestOffset - (oldestOffset % SEGMENT_SIZE);
        // Leave safety buffer of one segment
        offsetStopLimit -= SEGMENT_SIZE;

        // Log protection status changes
    } else if (previous_offset_was_known) {
        // Fall back to previous known values rather than emergency vacuum
        oldestOffset = prevOldestOffset;
        oldestOffsetKnown = true;
        offsetStopLimit = prevOffsetStopLimit;
    }

    // Update shared state with computed values
    LWLockAcquire(MultiXactGenLock, LW_EXCLUSIVE);
    MultiXactState->oldestOffset = oldestOffset;
    MultiXactState->oldestOffsetKnown = oldestOffsetKnown;
    MultiXactState->offsetStopLimit = offsetStopLimit;
    LWLockRelease(MultiXactGenLock);

    // Return true if emergency autovacuum is needed
    return !oldestOffsetKnown ||
           (nextOffset - oldestOffset > SAFE_THRESHOLD);
}
```

Key simplifications made:
- Consolidated variable declarations and removed compiler placation comments
- Abstracted segment size calculations into SEGMENT_SIZE constant references
- Simplified logging calls to high-level comments about their purpose
- Removed detailed error message text while preserving the logging structure
- Consolidated the fallback logic for missing offset data
- Simplified threshold constant names for readability
- Preserved the essential locking protocol and state management logic
- Maintained the core algorithm for determining emergency vacuum necessity