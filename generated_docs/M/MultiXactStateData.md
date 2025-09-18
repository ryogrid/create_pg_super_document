# MultiXactStateData

## Location
[src/backend/access/transam/multixact.c:241-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L241-L331)

## Overview
MultiXactStateData is a core data structure that maintains the shared state for PostgreSQL's multixact system across all backend processes, managing multixact allocation and tracking for shared lock coordination.

## Definition
```c
typedef struct MultiXactStateData
{
    MultiXactId nextMXact;
    MultiXactOffset nextOffset;
    bool finishedStartup;
    MultiXactId oldestMultiXactId;
    Oid oldestMultiXactDB;
    MultiXactOffset oldestOffset;
    bool oldestOffsetKnown;
    MultiXactId multiVacLimit;
    MultiXactId multiWarnLimit;
    MultiXactId multiStopLimit;
    MultiXactId multiWrapLimit;
    MultiXactOffset offsetStopLimit;
    ConditionVariable nextoff_cv;
    MultiXactId perBackendXactIds[FLEXIBLE_ARRAY_MEMBER];
} MultiXactStateData;
```

## Detailed Description
MultiXactStateData serves as the central control structure for PostgreSQL's MultiXact subsystem, which manages shared locks by allowing multiple transactions to hold different types of locks on the same tuple simultaneously. This structure is shared across all backend processes and protected by MultiXactGenLock to ensure thread-safe access.

The structure maintains critical state information including the next multixact ID to be assigned, offset tracking, vacuum-related limits for anti-wraparound protection, and per-backend arrays that track each backend's multixact participation. The design supports both multixact ID allocation and cleanup operations while preventing wraparound issues through various limit tracking mechanisms.

## Parameters / Member Variables
- `nextMXact`: Next MultiXactId to be assigned for new multixacts
- `nextOffset`: Next offset to be assigned in the multixact members table
- `finishedStartup`: Flag indicating whether multixact startup is complete
- `oldestMultiXactId`: Oldest multixact still potentially referenced by relations
- `oldestMultiXactDB`: Database OID containing the oldest multixact
- `oldestOffset`: Oldest multixact offset potentially still referenced
- `oldestOffsetKnown`: Flag indicating whether oldestOffset value is known
- `multiVacLimit`: Vacuum limit for multixact anti-wraparound measures
- `multiWarnLimit`: Warning threshold for multixact wraparound
- `multiStopLimit`: Stop limit to prevent multixact wraparound
- `multiWrapLimit`: Hard wraparound limit for multixacts
- `offsetStopLimit`: Stop limit for multixact member offsets
- `nextoff_cv`: Condition variable for sleeping until multixact offset is written
- `perBackendXactIds`: Flexible array containing per-backend multixact tracking arrays

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactId (type used throughout structure)
  - MultiXactOffset (type for offset tracking)
  - ConditionVariable (for synchronization)
  - FLEXIBLE_ARRAY_MEMBER (for variable-length backend arrays)
- Called from (representative examples):
  - MaxOldestSlot (uses structure for size calculations)
  - SHARED_MULTIXACT_STATE_SIZE (references structure for memory allocation)

## Notes and Other Information
The structure includes two important per-backend arrays stored after the main structure: OldestMemberMXactId[] tracks the oldest MultiXactId each backend could be a member of, while OldestVisibleMXactId[] tracks the oldest MultiXactId each backend considers potentially live. These arrays are crucial for vacuum operations and determining when multixact data can be safely truncated. The entire state is protected by MultiXactGenLock, and SLRU bank locks are used for buffer access synchronization.