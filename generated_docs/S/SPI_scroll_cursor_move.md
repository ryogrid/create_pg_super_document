# SPI_scroll_cursor_move

## Location
[src/backend/executor/spi.c:1850-1861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1850-L1861)

## Overview
Move the position of a scrollable cursor in PostgreSQL's Server Programming Interface (SPI) without fetching any data.

## Definition

```c
void
SPI_scroll_cursor_move(Portal portal, FetchDirection direction, long count)
```
## Detailed Description
SPI_scroll_cursor_move is a utility function that moves the position of a scrollable cursor without retrieving any rows. This function is useful when you need to reposition a cursor to a specific location before performing fetch operations. The function internally calls _SPI_cursor_operation with a None_Receiver destination, which means no data is retrieved - only the cursor position is changed.

The function works with Portal objects, which represent prepared statements or cursors in PostgreSQL. It supports various movement directions (forward, backward, absolute, relative) and can move by a specified number of rows.

## Parameters / Member Variables
- : A Portal object representing the cursor to be moved. Must be a valid, open cursor.
- : A FetchDirection enum value specifying how to move the cursor (e.g., FETCH_FORWARD, FETCH_BACKWARD, FETCH_ABSOLUTE, FETCH_RELATIVE).
- : The number of rows to move. For absolute positioning, this is the target row number; for relative positioning, this is the offset from the current position.

## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_cursor_operation](_SPI_cursor_operation.md)
  - [FetchDirection](../F/FetchDirection.md) (enum type)
  - [Portal](../P/Portal.md) (struct type)
  - None_Receiver (global variable)
- Called from (representative examples):
  - User-defined SPI functions that need cursor positioning

## Notes and Other Information
- This function does not return any data; it only changes the cursor position
- The cursor must be scrollable for this function to work properly
- Error handling is delegated to the underlying _SPI_cursor_operation function
- The function requires an active SPI connection to work
- Unlike SPI_cursor_fetch, this function does not populate SPI_processed or SPI_tuptable
- Commonly used in conjunction with SPI_cursor_fetch to position and then retrieve data