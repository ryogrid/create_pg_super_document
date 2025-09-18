# SPI_cursor_move

## Location
[src/backend/executor/spi.c:1821-1834](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1821-L1834)

## Overview
SPI_cursor_move changes the position of a cursor without retrieving any rows, effectively implementing cursor positioning operations similar to SQL MOVE statements.

## Definition


## Detailed Description
This function moves the cursor position by the specified number of rows in the given direction without actually fetching any data. It is similar to SPI_cursor_fetch but discards any rows that would be retrieved during the movement operation. The function uses None_Receiver as the destination, which causes the rows to be processed but not returned to the caller. This is useful for repositioning a cursor before subsequent fetch operations or for skipping over unwanted rows efficiently.

Like SPI_cursor_fetch, it wraps the internal _SPI_cursor_operation function but with a null destination receiver to discard the results. The SPI_processed variable is still updated to reflect the number of rows actually moved over.

## Parameters / Member Variables
- : The Portal handle for the cursor to move
- : Boolean indicating movement direction (true for forward, false for backward)
- : Number of rows to move over (positive number)

## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_cursor_operation](_SPI_cursor_operation.md) (internal cursor operation handler)
  - FETCH_FORWARD/FETCH_BACKWARD (fetch direction constants)
  - None_Receiver (null destination receiver that discards results)
- Called from (representative examples):
  - Limited direct usage in core PostgreSQL (mainly referenced in header files)

## Notes and Other Information
- The cursor position is updated but no row data is returned to the caller
- SPI_processed is set to the actual number of rows moved over (may be less than requested if cursor reaches end)
- Backward movement requires the cursor to have been opened with scroll capability
- More efficient than fetching and discarding rows when you only need to change cursor position
- Useful for implementing cursor navigation operations in procedural languages
- The function will move fewer rows than requested if the cursor reaches the beginning or end