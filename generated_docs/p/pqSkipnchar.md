# pqSkipnchar

## Location
[src/interfaces/libpq/fe-misc.c:187-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L187-L201)

## Overview
pqSkipnchar advances the input buffer cursor by a specified number of bytes without reading the data into a destination buffer.

## Definition
```c
int pqSkipnchar(size_t len, PGconn *conn)
```

## Detailed Description
pqSkipnchar is a utility function that skips over a specified number of bytes in the connection's input buffer by advancing the cursor position without actually copying the data anywhere. This is useful when processing PostgreSQL protocol messages where certain fields need to be ignored or when the data will be processed later by other functions.

The function performs the same availability check as pqGetnchar but only advances the cursor without doing any memory copying. According to the source comments, it's designed to provide the same debug output behavior as pqGetnchar while skipping the actual data reading. This makes it useful for protocol parsing where some fields may not be needed by the current operation.

## Parameters / Member Variables
- `len`: Number of bytes to skip in the input buffer
- `conn`: PGconn connection object containing the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - None (direct buffer manipulation only)
- Called from (representative examples):
  - [getAnotherTuple](../g/getAnotherTuple.md) (fe-protocol3.c:823)

## Notes and Other Information
- Returns 0 on success, EOF if insufficient data is available
- Does not copy or process the skipped data in any way
- Primarily useful for its debug output capabilities, which match pqGetnchar
- Used when data exists in the buffer but is not needed for the current operation
- Maintains the same error checking as data-reading functions for consistency
- Advances the input cursor position by exactly the specified number of bytes
- Commonly used in protocol message parsing where certain fields can be ignored