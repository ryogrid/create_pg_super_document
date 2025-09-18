# SPI_cursor_open_internal

## Location
[src/backend/executor/spi.c:1577-1793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1577-L1793)

## Overview
SPI_cursor_open_internal is the common internal function that implements cursor opening functionality for all SPI cursor open variants. It creates and starts a Portal for executing SELECT queries as cursors within the Server Programming Interface (SPI).

## Definition


## Detailed Description
This internal function handles the core logic for opening SPI cursors. It validates that the provided plan is suitable for cursor operations, creates a Portal with the specified name (or generates one automatically), configures cursor options including scroll behavior, handles parameter binding, and starts portal execution with the appropriate snapshot. The function ensures proper memory context management and error handling throughout the process.

The function performs extensive validation including checking that the plan contains only cursor-compatible queries (primarily SELECT statements), handling scroll cursor restrictions (disallowing SELECT FOR UPDATE with SCROLL), and validating read-only requirements when specified.

## Parameters / Member Variables
- : The name for the cursor portal (NULL or empty string generates a random name)
- : The prepared SPIPlan containing the query to execute as a cursor
- : Parameter list information for parameterized queries (can be NULL)
- : Boolean flag indicating if cursor should be restricted to read-only operations

## Dependencies
- Functions called/Symbols referenced:
  - [SPI_is_cursor_plan](SPI_is_cursor_plan.md) (validates plan is cursor-compatible)
  - CreateNewPortal/CreatePortal (creates the portal)
  - [GetCachedPlan](../G/GetCachedPlan.md) (retrieves cached execution plan)
  - [PortalDefineQuery](../P/PortalDefineQuery.md) (associates query with portal)
  - [PortalStart](../P/PortalStart.md) (begins portal execution)
  - GetActiveSnapshot/GetTransactionSnapshot (manages snapshots)
  - _SPI_begin_call/_SPI_end_call (SPI stack management)
- Called from (representative examples):
  - [SPI_cursor_open](SPI_cursor_open.md)
  - [SPI_cursor_open_with_args](SPI_cursor_open_with_args.md)
  - [SPI_cursor_open_with_paramlist](SPI_cursor_open_with_paramlist.md)
  - [SPI_cursor_parse_open](SPI_cursor_parse_open.md)

## Notes and Other Information
- This is a static function internal to spi.c and not part of the public SPI API
- Handles both saved and unsaved plans differently for memory management
- Automatically determines scroll behavior based on plan characteristics when not explicitly specified
- Enforces restriction that scrollable cursors must be read-only when using SELECT FOR UPDATE/SHARE
- Manages memory contexts carefully to prevent leaks, especially during error conditions
- Returns a Portal handle that can be used with other SPI cursor functions