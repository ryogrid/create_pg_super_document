# pqCheckInBufferSpace

## Location
src/interfaces/libpq/fe-misc.c: 351 - 457

## Overview
Ensures that the connection's input buffer has sufficient space to hold the specified number of bytes, with intelligent buffer compaction before reallocation.

## Definition
```c
int pqCheckInBufferSpace(size_t bytes_needed, PGconn *conn)
```

## Detailed Description
The `pqCheckInBufferSpace` function manages the input buffer for PostgreSQL connections with a more sophisticated approach than its output counterpart. Before attempting to reallocate memory, it first tries to reclaim space by compacting the buffer—moving any unprocessed data to the beginning and resetting the buffer pointers. This "left-justification" process can often satisfy space requirements without memory allocation.

If compaction is insufficient, the function employs the same two-phase reallocation strategy as `pqCheckOutBufferSpace`: first attempting to double the buffer size, then falling back to 8KB incremental growth. This optimization is particularly valuable for input buffers since they frequently contain partially processed data that can be compacted.

## Parameters / Member Variables
- `bytes_needed`: The total number of bytes that the input buffer must be able to hold (including any data already stored)
- `conn`: PostgreSQL connection object containing the input buffer and associated pointers

## Dependencies
- Functions called/Symbols referenced:
  - memmove (standard C library function for safe memory copying)
  - realloc (standard C library function for memory reallocation)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md) (adds error message to connection's error buffer)
- Called from (representative examples):
  - [pqReadData](pqReadData.md) (when reading data from the network)
  - [pqParseInput3](pqParseInput3.md) (during protocol message parsing)
  - [getCopyDataMessage](../g/getCopyDataMessage.md) (when handling COPY data)
  - [pqFunctionCall3](pqFunctionCall3.md) (during function call processing)

## Notes and Other Information
- Returns 0 on success, EOF on failure (memory allocation error)
- Performs buffer compaction before attempting reallocation, which often eliminates the need for memory allocation
- Adjusts inStart, inCursor, and inEnd pointers during compaction to maintain buffer state consistency
- Uses memmove for safe overlapping memory copy during compaction
- Handles the special case where the buffer is logically empty by resetting all pointers to zero
- More complex than pqCheckOutBufferSpace due to the need to preserve existing data while optimizing space usage
- Critical for efficient network data processing in the PostgreSQL client library