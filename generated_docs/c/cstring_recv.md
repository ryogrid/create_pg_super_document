# cstring_recv

## Location
src/backend/utils/adt/pseudotypes.c: 123 - 133

## Overview
The `cstring_recv` function is a binary input (receive) conversion function for the `cstring` pseudo-type in PostgreSQL, deserializing cstring data from PostgreSQL's binary protocol format.

## Definition
```c
Datum cstring_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `cstring_recv` function serves as the binary input conversion function for PostgreSQL's `cstring` pseudo-type. It receives binary data through PostgreSQL's message protocol system and converts it into a PostgreSQL cstring. The function uses `pq_getmsgtext()` to extract text data from the incoming message buffer, reading all remaining bytes in the buffer (from current cursor position to the end). This function is part of the complete I/O function suite for the `cstring` pseudo-type, specifically handling binary protocol communication between PostgreSQL clients and servers.

## Parameters / Member Variables
- The function follows PostgreSQL's standard function calling convention using `PG_FUNCTION_ARGS`, which provides access to:
  - Input parameter: A `StringInfo` buffer containing binary message data, obtained via `PG_GETARG_POINTER(0)`
- Local variables:
  - `str`: Pointer to the extracted string data
  - `nbytes`: Number of bytes read from the message buffer

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POINTER` (macro for extracting pointer argument)
  - `pq_getmsgtext` (function to extract text from message buffer)
  - `PG_RETURN_CSTRING` (macro for returning cstring result)
- Called from (representative examples):
  - PostgreSQL's binary protocol message handling
  - Type system operations during binary data deserialization

## Notes and Other Information
- This function is part of the binary I/O functions for the `cstring` pseudo-type, complementing the text-based I/O functions
- The function reads all remaining data in the message buffer (from cursor to end)
- Used in PostgreSQL's binary protocol communication for efficient data transfer
- Located in `src/backend/utils/adt/pseudotypes.c:123-133`
- The `StringInfo` parameter represents a buffer with current position tracking via the cursor field