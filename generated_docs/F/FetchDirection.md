# FetchDirection

## Location
[src/include/nodes/parsenodes.h:3324-3325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3324-L3325)

## Overview
FetchDirection is an enumeration type that specifies the direction and positioning behavior for FETCH and MOVE SQL statements when operating on cursors in PostgreSQL.

## Definition


## Detailed Description
FetchDirection defines the four possible movement patterns for cursor operations in PostgreSQL:

- **FETCH_FORWARD/FETCH_BACKWARD**: These modes fetch a specified number of rows in the forward or backward direction from the current cursor position. The  parameter indicates the number of rows to retrieve, with FETCH_ALL (LONG_MAX) meaning to fetch all remaining rows.

- **FETCH_ABSOLUTE/FETCH_RELATIVE**: These modes position the cursor at a specific location and fetch only one row. FETCH_ABSOLUTE positions at an absolute row number, while FETCH_RELATIVE positions relative to the current cursor position.

This enumeration is used in conjunction with FetchStmt structures to implement SQL FETCH and MOVE commands that allow navigation through result sets via cursors.

## Parameters / Member Variables
- : Fetch rows moving forward from current position
- : Fetch rows moving backward from current position  
- : Position cursor at absolute row number and fetch one row
- : Position cursor relative to current position and fetch one row

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - [SPI_scroll_cursor_fetch](../S/SPI_scroll_cursor_fetch.md) (src/backend/executor/spi.c:1835)
  - [SPI_scroll_cursor_move](../S/SPI_scroll_cursor_move.md) (src/backend/executor/spi.c:1850)
  - [PortalRunFetch](../P/PortalRunFetch.md) (src/backend/tcop/pquery.c:1381)
  - [DoPortalRunFetch](../D/DoPortalRunFetch.md) (src/backend/tcop/pquery.c:1479)
  - FetchStmt (src/include/nodes/parsenodes.h:3331)

## Notes and Other Information
- The FETCH_ALL constant is defined as LONG_MAX to represent fetching all available rows
- This enumeration is closely tied to SQL cursor functionality and the SPI (Server Programming Interface)
- Used by both internal PostgreSQL cursor operations and external cursor manipulation via SPI functions
- The distinction between directional fetching (FORWARD/BACKWARD) and positional fetching (ABSOLUTE/RELATIVE) reflects SQL standard cursor navigation capabilities