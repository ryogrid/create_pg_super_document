# PortalRunFetch

## Location
src/backend/tcop/pquery.c: 1380 - 1477

## Overview
A variant form of PortalRun that supports SQL FETCH directions, enabling cursor-like fetching of results with directional control and count limits.

## Definition


## Detailed Description
PortalRunFetch provides cursor-style access to portal results, supporting SQL FETCH operations with directional control (forward, backward, absolute, relative). The function handles different portal strategies by either delegating directly to DoPortalRunFetch for PORTAL_ONE_SELECT, or first ensuring results are stored via FillPortalStore for other strategies before fetching. It maintains proper portal state management, resource ownership context, and error handling with cleanup. The function returns the number of rows processed and supports special count values like FETCH_ALL. It includes comprehensive error handling that properly marks portals as failed and restores global state on exceptions.

## Parameters / Member Variables
- : The Portal structure to fetch results from
- : FetchDirection enum specifying the fetch direction (forward, backward, absolute, relative)
- : Number of rows to fetch (0 or negative means no-op, FETCH_ALL means all rows)
- : DestReceiver that will process the fetched tuples

## Dependencies
- Functions called/Symbols referenced:
  - PortalIsValid
  - MarkPortalActive
  - [DoPortalRunFetch](../D/DoPortalRunFetch.md)
  - [FillPortalStore](../F/FillPortalStore.md)
  - MarkPortalFailed
  - PG_TRY/PG_CATCH/PG_END_TRY macros
- Called from (representative examples):
  - [PerformPortalFetch](PerformPortalFetch.md)
  - [_SPI_cursor_operation](../S/_SPI_cursor_operation.md)

## Notes and Other Information
- This function is public (not static) and declared in pquery.h
- Returns uint64 representing the number of rows processed
- Assumes no callers want isTopLevel = true for the portal execution
- Handles count <= 0 as a no-op that starts up and shuts down the destination
- Uses proper exception handling to ensure portal state is correctly maintained
- Switches memory context to portal context during execution
- Saves and restores global portal state variables (ActivePortal, CurrentResourceOwner, PortalContext)
- For non-SELECT strategies, lazily fills the portal store if not already done
- Marks portal as PORTAL_READY after successful execution
- Used primarily for implementing SQL FETCH commands and SPI cursor operations