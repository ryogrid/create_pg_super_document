# DoPortalRewind

## Location
[src/backend/tcop/pquery.c:1672-1717](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L1672-L1717)

## Overview
DoPortalRewind resets a portal to its starting position, allowing re-execution of the query from the beginning while respecting cursor scroll options.

## Definition

```c
static void
DoPortalRewind(Portal portal)
```
## Detailed Description
DoPortalRewind implements the functionality to rewind a portal back to its initial state. This operation is essential for cursor operations that need to restart from the beginning of the result set. The function handles both held (materialized) portals that use tuple stores and active portals that require executor rewinding.

The function performs several key operations:
1. Validates that the cursor allows scrolling (enforces NO SCROLL restrictions)
2. Rewinds the tuple store if the portal has materialized results
3. Rewinds the executor state if the portal has an active query
4. Resets portal positioning flags and counters

The function is optimized to skip unnecessary work when the portal is already at the start position and hasn't been advanced.

## Parameters / Member Variables
- `portal`: The portal to rewind to its starting position
## Dependencies
- Functions called/Symbols referenced:
  - [tuplestore_rescan](../t/tuplestore_rescan.md)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)
  - [ExecutorRewind](../E/ExecutorRewind.md)  
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [Portal](../P/Portal.md) structure fields (atStart, atEnd, portalPos, holdStore, holdContext, queryDesc, cursorOptions)
  - [QueryDesc](../Q/QueryDesc.md) structure
  - CURSOR_OPT_NO_SCROLL constant
- Called from (representative examples):
  - [DoPortalRunFetch](DoPortalRunFetch.md) (multiple call sites for absolute positioning and optimization)

## Notes and Other Information
- Enforces NO SCROLL cursor restrictions by throwing an error if backward scanning is attempted on a forward-only cursor
- Handles both materialized portals (with holdStore) and active executor-based portals
- Properly manages memory contexts when accessing tuple stores
- Maintains snapshot consistency during executor rewind operations
- Optimizes by avoiding work when the portal is already in the starting state
- Critical for implementing SQL cursor semantics, particularly FETCH ABSOLUTE and cursor rewinding operations

## Simplified Source

```c
static void DoPortalRewind(Portal portal) {
    // Skip work if portal is already at start and hasn't moved
    if (portal->atStart && !portal->atEnd)
        return;

    // Check if cursor allows backward scanning
    if (portal->cursorOptions & CURSOR_OPT_NO_SCROLL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("cursor can only scan forward"),
                 errhint("Declare it with SCROLL option to enable backward scan.")));

    // Rewind the tuple store if materialized results exist
    if (portal->holdStore) {
        MemoryContext oldcontext = MemoryContextSwitchTo(portal->holdContext);
        tuplestore_rescan(portal->holdStore);
        MemoryContextSwitchTo(oldcontext);
    }

    // Rewind the executor if active query exists
    QueryDesc *queryDesc = portal->queryDesc;
    if (queryDesc) {
        PushActiveSnapshot(queryDesc->snapshot);
        ExecutorRewind(queryDesc);
        PopActiveSnapshot();
    }

    // Reset portal position flags
    portal->atStart = true;
    portal->atEnd = false;
    portal->portalPos = 0;
}
```