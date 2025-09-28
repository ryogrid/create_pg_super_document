# PortalRunSelect

## Location
[src/backend/tcop/pquery.c:865-997](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L865-L997)

## Overview
Executes a portal's query in SELECT mode, handling both forward and backward sequential access to fetch tuples from either the executor or a held store.

## Definition
static uint64 PortalRunSelect(Portal portal, bool forward, long count, DestReceiver *dest)

## Detailed Description
PortalRunSelect is the core function for executing SELECT-style queries through portals, supporting both PORTAL_ONE_SELECT mode and fetching from completed holdStore in PORTAL_ONE_RETURNING, PORTAL_ONE_MOD_WITH, and PORTAL_UTIL_SELECT cases. It handles simple N-rows-forward-or-backward access patterns and manages portal position state.

The function determines scan direction based on the forward parameter and current portal state, then either runs the executor directly or fetches from a held tuple store. It carefully manages portal position markers (atStart, atEnd, portalPos) and validates scroll capabilities for backward scans. The function supports both live execution through ExecutorRun and fetching from stored results through RunFromStore.

## Parameters / Member Variables
- portal: The Portal to execute, must have either a ready queryDesc or holdStore
- forward: true for forward scan, false for backward scan
- count: Maximum number of rows to fetch; FETCH_ALL means all rows, count <= 0 is a no-op
- dest: DestReceiver where fetched tuples should be sent

## Dependencies
- Functions called/Symbols referenced:
  - [RunFromStore](../R/RunFromStore.md)
  - [PushActiveSnapshot](PushActiveSnapshot.md)
  - [ExecutorRun](../E/ExecutorRun.md)
  - [PopActiveSnapshot](PopActiveSnapshot.md)
  - ScanDirectionIsNoMovement
- Called from (representative examples):
  - [PortalRun](PortalRun.md)
  - [DoPortalRunFetch](../D/DoPortalRunFetch.md) (multiple locations)

## Notes and Other Information
- Returns the number of rows processed, suitable for use in result tags
- Handles both live query execution and fetching from held cursor data
- Validates scroll permissions for backward scans, requiring CURSOR_OPT_SCROLL
- Manages portal position state including atStart, atEnd, and portalPos counters
- Forces queryDesc destination to match the provided dest parameter on each call
- Uses NoMovementScanDirection when already at boundary or count <= 0
- Supports FETCH_ALL by converting to count = 0 for the executor
- Located in src/backend/tcop/pquery.c:865-997

## Simplified Source

```c
// Simplified version of PortalRunSelect
static uint64 PortalRunSelect(Portal portal, bool forward, long count, DestReceiver *dest) {
    QueryDesc *queryDesc = portal->queryDesc;
    ScanDirection direction;
    uint64 nprocessed;

    // Must have either a ready query or held data
    Assert(queryDesc || portal->holdStore);

    // Set destination for the query
    if (queryDesc)
        queryDesc->dest = dest;

    if (forward) {
        // Forward scan logic
        if (portal->atEnd || count <= 0) {
            direction = NoMovementScanDirection;
            count = 0;
        } else {
            direction = ForwardScanDirection;
        }

        // Handle FETCH_ALL
        if (count == FETCH_ALL)
            count = 0;

        // Execute from store or live query
        if (portal->holdStore) {
            nprocessed = RunFromStore(portal, direction, (uint64) count, dest);
        } else {
            PushActiveSnapshot(queryDesc->snapshot);
            ExecutorRun(queryDesc, direction, (uint64) count, false);
            nprocessed = queryDesc->estate->es_processed;
            PopActiveSnapshot();
        }

        // Update portal position for forward scan
        if (!ScanDirectionIsNoMovement(direction)) {
            if (nprocessed > 0)
                portal->atStart = false;
            if (count == 0 || nprocessed < (uint64) count)
                portal->atEnd = true;
            portal->portalPos += nprocessed;
        }
    } else {
        // Backward scan logic
        if (portal->cursorOptions & CURSOR_OPT_NO_SCROLL) {
            ereport(ERROR,
                   (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("cursor can only scan forward"),
                    errhint("Declare it with SCROLL option to enable backward scan.")));
        }

        if (portal->atStart || count <= 0) {
            direction = NoMovementScanDirection;
            count = 0;
        } else {
            direction = BackwardScanDirection;
        }

        // Handle FETCH_ALL
        if (count == FETCH_ALL)
            count = 0;

        // Execute from store or live query
        if (portal->holdStore) {
            nprocessed = RunFromStore(portal, direction, (uint64) count, dest);
        } else {
            PushActiveSnapshot(queryDesc->snapshot);
            ExecutorRun(queryDesc, direction, (uint64) count, false);
            nprocessed = queryDesc->estate->es_processed;
            PopActiveSnapshot();
        }

        // Update portal position for backward scan
        if (!ScanDirectionIsNoMovement(direction)) {
            if (nprocessed > 0 && portal->atEnd) {
                portal->atEnd = false;
                portal->portalPos++;
            }
            if (count == 0 || nprocessed < (uint64) count) {
                portal->atStart = true;
                portal->portalPos = 0;
            } else {
                portal->portalPos -= nprocessed;
            }
        }
    }

    return nprocessed;
}
```

Key simplifications made:
- Removed detailed comments while preserving essential logic
- Consolidated variable declarations
- Maintained the forward/backward scan distinction
- Preserved scroll validation and error handling
- Kept essential portal position management
- Focused on core workflow: determine direction, execute, update position
- Maintained critical safety checks and snapshot management