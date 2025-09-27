# MultiXactShmemInit

## Location
[src/backend/access/transam/multixact.c:1956-2005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L1956-L2005)

## Overview
This function initializes the shared memory structures and Simple LRU (SLRU) buffers required for multixact operations during PostgreSQL startup.

## Definition
```c
void MultiXactShmemInit(void)
```

## Detailed Description
MultiXactShmemInit is responsible for setting up all shared memory components needed for multixact functionality. It initializes two separate SLRU structures: one for multixact offsets and another for multixact members. The function also establishes the shared multixact state structure and sets up per-backend tracking arrays. During postmaster startup, it zeros out the shared state and initializes condition variables, while in backend processes it verifies the shared structures already exist.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - debug_elog2
  - [SimpleLruInit](../S/SimpleLruInit.md) (called twice)
  - [SlruPagePrecedesUnitTests](../S/SlruPagePrecedesUnitTests.md)
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - MemSet
  - [ConditionVariableInit](../C/ConditionVariableInit.md)
  - [MultiXactOffsetPagePrecedes](MultiXactOffsetPagePrecedes.md)
  - [MultiXactMemberPagePrecedes](MultiXactMemberPagePrecedes.md)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md)
  - Referenced in SizeOfMultiXactTruncate

## Notes and Other Information
- Initializes two SLRU structures: MultiXactOffsetCtl for offsets and MultiXactMemberCtl for members
- The offset SLRU uses "pg_multixact/offsets" directory while member SLRU uses "pg_multixact/members"
- Sets up page precedence functions for proper SLRU ordering
- Only runs unit tests for the offset SLRU (member SLRU doesn't meet criteria)
- Different behavior for postmaster vs backend processes - postmaster initializes, backends verify
- Establishes global arrays OldestMemberMXactId and OldestVisibleMXactId for per-backend tracking
- Uses condition variables for coordination between processes
- Part of the broader shared memory initialization during PostgreSQL startup
- Located in src/backend/access/transam/multixact.c:1956-2005

## Simplified Source

```c
// Simplified version of MultiXactShmemInit
void MultiXactShmemInit(void) {
    bool found;

    // Step 1: Set up page precedence functions for SLRU ordering
    MultiXactOffsetCtl->PagePrecedes = MultiXactOffsetPagePrecedes;
    MultiXactMemberCtl->PagePrecedes = MultiXactMemberPagePrecedes;

    // Step 2: Initialize offset SLRU (stores multixact-to-offset mappings)
    SimpleLruInit(MultiXactOffsetCtl,
                  "multixact_offset", multixact_offset_buffers, 0,
                  "pg_multixact/offsets", /* other parameters */);
    SlruPagePrecedesUnitTests(MultiXactOffsetCtl, MULTIXACT_OFFSETS_PER_PAGE);

    // Step 3: Initialize member SLRU (stores multixact member lists)
    SimpleLruInit(MultiXactMemberCtl,
                  "multixact_member", multixact_member_buffers, 0,
                  "pg_multixact/members", /* other parameters */);

    // Step 4: Initialize shared multixact state structure
    MultiXactState = ShmemInitStruct("Shared MultiXact State",
                                    SHARED_MULTIXACT_STATE_SIZE,
                                    &found);

    // Step 5: Initialize state for postmaster, verify for backends
    if (!IsUnderPostmaster) {
        // Postmaster: initialize the shared state
        MemSet(MultiXactState, 0, SHARED_MULTIXACT_STATE_SIZE);
        ConditionVariableInit(&MultiXactState->nextoff_cv);
    }

    // Step 6: Set up global per-backend tracking arrays
    OldestMemberMXactId = MultiXactState->perBackendXactIds;
    OldestVisibleMXactId = OldestMemberMXactId + MaxOldestSlot;
}
```

Key simplifications made:
- Removed debug logging call for clarity
- Consolidated SLRU initialization parameters with comments
- Removed detailed parameter lists for SimpleLruInit calls
- Simplified the postmaster vs backend logic explanation
- Removed assertion checks to focus on main functionality
- Added step-by-step comments for the main initialization phases
- Focused on the core purpose of each initialization step