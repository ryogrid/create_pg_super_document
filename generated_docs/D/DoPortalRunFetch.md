# DoPortalRunFetch

## Location
[src/backend/tcop/pquery.c:1478-1671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L1478-L1671)

## Overview
DoPortalRunFetch implements the core logic for fetching rows from a portal with support for various fetch directions (forward, backward, absolute, relative) and different count values.

## Definition

```c
static uint64
DoPortalRunFetch(Portal portal,
				 FetchDirection fdirection,
				 long count,
				 DestReceiver *dest)
```
## Detailed Description
DoPortalRunFetch is the internal implementation that handles the complexity of portal row fetching operations. It serves as the guts of PortalRunFetch with the portal context already established. The function supports multiple fetch directions and interprets negative count values as direction reversals. It handles special cases like FETCH_ALL (retrieving all remaining rows) and optimizes various fetch patterns.

The function implements SQL cursor semantics for different fetch operations:
- FETCH_FORWARD/FETCH_BACKWARD: Standard directional fetching
- FETCH_ABSOLUTE: Positioning to an absolute row number from the start
- FETCH_RELATIVE: Moving relative to the current position
- Zero count handling: Re-fetching the current row per SQL standard

For absolute positioning, the function optimizes by choosing whether to rewind and scan forward or scan from the current position based on which approach requires fewer row movements.

## Parameters / Member Variables
- `portal`: The portal from which to fetch rows
- `fdirection`: The fetch direction (FETCH_FORWARD, FETCH_BACKWARD, FETCH_ABSOLUTE, FETCH_RELATIVE)
- `count`: Number of rows to fetch; negative values reverse direction, FETCH_ALL means all rows
- `*dest`: Destination receiver for the fetched rows
## Dependencies
- Functions called/Symbols referenced:
  - [DoPortalRewind](DoPortalRewind.md)
  - [PortalRunSelect](../P/PortalRunSelect.md)
  - [Portal](../P/Portal.md) structure and its fields (strategy, portalPos, atStart, atEnd)
  - [FetchDirection](../F/FetchDirection.md) enum values
  - [DestReceiver](DestReceiver.md) and DestNone
  - [Portal](../P/Portal.md) strategy constants (PORTAL_ONE_SELECT, PORTAL_ONE_RETURNING, etc.)
- Called from (representative examples):
  - [PortalRunFetch](../P/PortalRunFetch.md)

## Notes and Other Information
- The function enforces NO SCROLL cursor restrictions by disallowing backwards movement
- Optimizes MOVE BACKWARD ALL operations by converting them to rewind operations
- Handles edge cases like fetching when positioned at the end of the result set
- Returns the number of rows processed, suitable for use in SQL result tags
- Uses None_Receiver for internal positioning operations that don't need to return data to the client

## Simplified Source

```c
static uint64 DoPortalRunFetch(Portal portal, FetchDirection fdirection,
                              long count, DestReceiver *dest) {
    bool forward;

    // Normalize direction and count
    switch (fdirection) {
        case FETCH_FORWARD:
        case FETCH_BACKWARD:
            if (count < 0) {
                fdirection = (fdirection == FETCH_FORWARD) ? FETCH_BACKWARD : FETCH_FORWARD;
                count = -count;
            }
            break;

        case FETCH_ABSOLUTE:
            if (count > 0) {
                // Position to absolute row number from start
                if ((uint64)(count - 1) <= portal->portalPos / 2 ||
                    portal->portalPos >= (uint64)LONG_MAX) {
                    DoPortalRewind(portal);
                    if (count > 1)
                        PortalRunSelect(portal, true, count - 1, None_Receiver);
                } else {
                    // More efficient to scan from current position
                    long pos = (long)portal->portalPos;
                    if (portal->atEnd) pos++;
                    if (count <= pos)
                        PortalRunSelect(portal, false, pos - count + 1, None_Receiver);
                    else if (count > pos + 1)
                        PortalRunSelect(portal, true, count - pos - 1, None_Receiver);
                }
                return PortalRunSelect(portal, true, 1L, dest);
            } else if (count < 0) {
                // Position from end
                PortalRunSelect(portal, true, FETCH_ALL, None_Receiver);
                if (count < -1)
                    PortalRunSelect(portal, false, -count - 1, None_Receiver);
                return PortalRunSelect(portal, false, 1L, dest);
            } else {
                // count == 0: rewind to start
                DoPortalRewind(portal);
                return PortalRunSelect(portal, true, 0L, dest);
            }

        case FETCH_RELATIVE:
            if (count > 0) {
                if (count > 1)
                    PortalRunSelect(portal, true, count - 1, None_Receiver);
                return PortalRunSelect(portal, true, 1L, dest);
            } else if (count < 0) {
                if (count < -1)
                    PortalRunSelect(portal, false, -count - 1, None_Receiver);
                return PortalRunSelect(portal, false, 1L, dest);
            } else {
                fdirection = FETCH_FORWARD;
            }
            break;
    }

    forward = (fdirection == FETCH_FORWARD);

    // Handle count == 0 (re-fetch current row)
    if (count == 0) {
        bool on_row = (!portal->atStart && !portal->atEnd);
        if (dest->mydest == DestNone)
            return on_row ? 1 : 0;
        if (on_row) {
            PortalRunSelect(portal, false, 1L, None_Receiver);
            count = 1;
            forward = true;
        }
    }

    // Optimize MOVE BACKWARD ALL
    if (!forward && count == FETCH_ALL && dest->mydest == DestNone) {
        uint64 result = portal->portalPos;
        if (result > 0 && !portal->atEnd) result--;
        DoPortalRewind(portal);
        return result;
    }

    return PortalRunSelect(portal, forward, count, dest);
}
```