# pqGetc

## Location
src/interfaces/libpq/fe-misc.c: 77 - 91

## Overview
Reads a single character from the PostgreSQL connection's input buffer.

## Definition


## Detailed Description
pqGetc is a low-level function used internally by libpq to read a single character from the connection's input buffer. It operates on data that has already been received from the backend and stored in the connection's inBuffer. The function does not perform any network I/O; it simply extracts the next available character from the buffered data.

The function uses the connection's inCursor to track the current reading position and inEnd to determine if there is available data. If data is available, it extracts one character and advances the cursor. If no data is available in the buffer, it returns EOF to indicate that more data needs to be read from the network.

This function is part of the internal protocol parsing infrastructure and is used extensively by higher-level protocol parsing functions to process messages received from the PostgreSQL backend.

## Parameters / Member Variables
- : Pointer to a char where the read character will be stored
- : Pointer to the PGconn structure representing the database connection

## Dependencies
- Functions called/Symbols referenced:
  - conn->inBuffer (connection input buffer)
  - conn->inCursor (current read position)
  - conn->inEnd (end of available data)
- Called from (representative examples):
  - [pqParseInput3](pqParseInput3.md) (protocol 3 message parsing)
  - [pqGetErrorNotice3](pqGetErrorNotice3.md) (error/notice message parsing)
  - [getCopyStart](../g/getCopyStart.md) (COPY operation message parsing)
  - [getReadyForQuery](../g/getReadyForQuery.md) (ready-for-query message parsing)
  - [getCopyDataMessage](../g/getCopyDataMessage.md) (COPY data message parsing)
  - [pqFunctionCall3](pqFunctionCall3.md) (function call result parsing)

## Notes and Other Information
- Returns 0 on success, EOF when no more data is available in the buffer
- EOF does not necessarily indicate a hard error, just that the buffer needs to be refilled
- This is an internal libpq function, not part of the public API
- Part of the low-level protocol parsing infrastructure
- Does not perform any network I/O operations
- Thread-safety depends on the connection's locking mechanisms