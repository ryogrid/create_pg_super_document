# bytearecv

## Location
src/backend/utils/adt/varlena.c: 471 - 489

## Overview
Converts binary data received from the PostgreSQL wire protocol (external binary format) into an internal bytea data structure.

## Definition
```c
Datum bytearecv(PG_FUNCTION_ARGS)
```

## Detailed Description
The bytearecv function is part of PostgreSQL's input/output system for the bytea data type. It handles the reception of binary data transmitted over the wire protocol in binary format. The function takes a StringInfo buffer containing the raw binary data and creates a proper bytea structure with the appropriate TOAST header.

The function calculates the number of bytes available in the input buffer, allocates memory for a new bytea structure (including space for the variable-length header), sets the proper size information, and copies the binary data from the input buffer to the newly allocated bytea structure.

## Parameters / Member Variables
- Input: StringInfo buffer obtained via `PG_GETARG_POINTER(0)` - contains binary data from wire protocol
- Returns: bytea structure via `PG_RETURN_BYTEA_P()`

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER
  - palloc
  - SET_VARSIZE
  - pq_copymsgbytes
  - VARDATA
  - PG_RETURN_BYTEA_P
- Constants referenced:
  - VARHDRSZ
- Called from:
  - (No direct references found - typically called by PostgreSQL's type input/output system)

## Notes and Other Information
- This is the binary input function for bytea type, complementing the text input function
- The function assumes the entire remaining buffer content represents the bytea value
- Used internally by PostgreSQL when receiving binary-format bytea data from clients
- The allocated bytea structure includes proper TOAST headers for variable-length data management
- Memory allocation uses palloc, which is PostgreSQL's memory context-aware allocator