# SPI_scroll_cursor_fetch

## Location
[src/backend/executor/spi.c:1835-1849](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1835-L1849)

## Overview
SPI_scroll_cursor_fetch provides advanced cursor navigation by allowing fetches in multiple directions using explicit FetchDirection constants, offering more control than the simple forward/backward boolean used by SPI_cursor_fetch.

## Definition


## Detailed Description
This function extends the basic SPI_cursor_fetch functionality by accepting a FetchDirection enum parameter instead of a simple boolean, allowing for more sophisticated cursor movement patterns. It supports the full range of fetch directions including FETCH_FORWARD, FETCH_BACKWARD, FETCH_ABSOLUTE, FETCH_RELATIVE, and positioning operations like FETCH_FIRST and FETCH_LAST. This makes it particularly useful for implementing scrollable cursor operations that require precise positioning.

Like other SPI cursor functions, it wraps _SPI_cursor_operation and uses the DestSPI receiver to capture results in the standard SPI result variables. The function is designed for cursors that have been opened with scrolling capability.

## Parameters / Member Variables
- : The Portal handle for the scrollable cursor
- : FetchDirection enum specifying the type and direction of movement (e.g., FETCH_FORWARD, FETCH_BACKWARD, FETCH_ABSOLUTE, FETCH_RELATIVE, FETCH_FIRST, FETCH_LAST)
- : Number of rows or position value (interpretation depends on direction type)

## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_cursor_operation](_SPI_cursor_operation.md) (internal cursor operation handler)
  - [CreateDestReceiver](../C/CreateDestReceiver.md) (creates destination for query results)
  - DestSPI (SPI destination receiver type)
  - [FetchDirection](../F/FetchDirection.md) (enum type for cursor directions)
- Called from (representative examples):
  - Limited direct usage in core PostgreSQL (mainly referenced in header files)

## Notes and Other Information
- Requires the cursor to have been opened with scroll capability for most direction types
- The count parameter meaning varies by direction: for FETCH_FORWARD/FETCH_BACKWARD it's the number of rows, for FETCH_ABSOLUTE it's the absolute row position, for FETCH_RELATIVE it's the relative offset
- Results are stored in SPI_tuptable and SPI_processed following standard SPI conventions
- More powerful than SPI_cursor_fetch but requires understanding of FetchDirection semantics
- FETCH_FIRST and FETCH_LAST ignore the count parameter
- The DestSPI receiver handles memory management automatically
- Provides the foundation for implementing full SQL cursor functionality in procedural languages