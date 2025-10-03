# _SPI_cursor_operation

## Location
[src/backend/executor/spi.c:3007-3057](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L3007-L3057)

## Overview
 is an internal SPI function that performs FETCH or MOVE operations on a cursor, handling the complete lifecycle of cursor operations within the SPI framework.

## Definition

```c
static void
_SPI_cursor_operation(Portal portal, FetchDirection direction, long count,
					  DestReceiver *dest)
```
## Detailed Description
This function implements the core logic for cursor operations (FETCH and MOVE) in the SPI framework. It manages the SPI call stack, validates the portal, executes the cursor operation via PortalRunFetch, and properly handles result processing. The function includes important safeguards against SPI stack corruption that can occur when the portal contains functions that themselves use SPI.

The function carefully manages the SPI global state by resetting result variables before the operation and properly updating them afterward. It also performs consistency checks on tuple counts when the destination is SPI.

## Parameters / Member Variables
- `portal`: Portal representing the cursor to operate on
- `direction`: Direction for the fetch/move operation (forward/backward)
- `count`: Number of rows to fetch or move (positive/negative values)
- `*dest`: Destination receiver for the fetched tuples
## Dependencies
- Functions called/Symbols referenced:
  - PortalIsValid: Validates that the portal is still valid
  - [_SPI_begin_call](_SPI_begin_call.md): Initializes SPI call context
  - [PortalRunFetch](../P/PortalRunFetch.md): Executes the actual cursor fetch/move operation
  - [_SPI_checktuples](_SPI_checktuples.md): Validates SPI tuple count consistency
  - [_SPI_end_call](_SPI_end_call.md): Cleans up SPI call context
- Called from (representative examples):
  - [SPI_cursor_fetch](SPI_cursor_fetch.md): Public API for fetching from cursor
  - [SPI_cursor_move](SPI_cursor_move.md): Public API for moving cursor position
  - [SPI_scroll_cursor_fetch](SPI_scroll_cursor_fetch.md): Scrollable cursor fetch operations
  - [SPI_scroll_cursor_move](SPI_scroll_cursor_move.md): Scrollable cursor move operations

## Notes and Other Information
- Includes important comment about SPI stack pointer stability during portal execution
- Resets SPI_processed and SPI_tuptable before operation and restores them afterward
- Transfers ownership of tuptable to caller by setting _SPI_current->tuptable to NULL
- Performs consistency checks only when destination is DestSPI
- Uses _SPI_begin_call/end_call pair to properly manage SPI call stack
- The nfetched assignment is deliberately separated from PortalRunFetch call due to potential SPI stack movement