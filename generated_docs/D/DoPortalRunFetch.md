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
- : The portal from which to fetch rows
- : The fetch direction (FETCH_FORWARD, FETCH_BACKWARD, FETCH_ABSOLUTE, FETCH_RELATIVE)
- : Number of rows to fetch; negative values reverse direction, FETCH_ALL means all rows
- : Destination receiver for the fetched rows

## Dependencies
- Functions called/Symbols referenced:
  - [DoPortalRewind](DoPortalRewind.md)
  - [PortalRunSelect](../P/PortalRunSelect.md)
  - [Portal](../P/Portal.md) structure and its fields (strategy, portalPos, atStart, atEnd)
  - [FetchDirection](../F/FetchDirection.md) enum values
  - DestReceiver and DestNone
  - [Portal](../P/Portal.md) strategy constants (PORTAL_ONE_SELECT, PORTAL_ONE_RETURNING, etc.)
- Called from (representative examples):
  - [PortalRunFetch](../P/PortalRunFetch.md)

## Notes and Other Information
- The function enforces NO SCROLL cursor restrictions by disallowing backwards movement
- Optimizes MOVE BACKWARD ALL operations by converting them to rewind operations
- Handles edge cases like fetching when positioned at the end of the result set
- Returns the number of rows processed, suitable for use in SQL result tags
- Uses None_Receiver for internal positioning operations that don't need to return data to the client