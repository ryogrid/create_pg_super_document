# PQExpBufferData

## Location
[src/interfaces/libpq/pqexpbuffer.h:44-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/pqexpbuffer.h#L44-L49)

## Overview
PQExpBufferData is a struct that holds information about an extensible string buffer used throughout PostgreSQL's libpq client library for dynamic string construction and manipulation.

## Definition


## Detailed Description
PQExpBufferData is the core data structure for PostgreSQL's extensible string buffer system. It manages dynamically allocated string buffers that can grow as needed to accommodate string data of varying lengths. This structure is fundamental to libpq's string handling operations and is used extensively throughout the PostgreSQL client libraries for building SQL queries, formatting output, and handling various text processing tasks.

The buffer maintains both the current string length and the allocated buffer size, allowing for efficient memory management and preventing buffer overruns. When the buffer capacity is exceeded, the system can reallocate a larger buffer and copy the existing data.

In error conditions (such as memory allocation failures), the buffer enters a "broken" state where data points to a statically allocated empty string and both len and maxlen are set to 0.

## Parameters / Member Variables
- : Pointer to the current buffer for the string (allocated with malloc). Contains a null-terminated string with guaranteed '\0' at data[len]. In error conditions, points to a statically allocated empty string.
- : Current string length in bytes. Always includes the terminating null character in calculations. Set to 0 in error conditions.
- : Allocated size in bytes of the 'data' buffer, representing the maximum string size (including terminating '\0') that can be stored without reallocation. Must always be greater than len except in error conditions where it equals 0.

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a plain data structure)
- Called from (representative examples):
  - createPQExpBuffer (buffer creation)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (string appending operations)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md) (buffer reset operations)
  - Various libpq functions throughout PostgreSQL client utilities
  - Used extensively in pg_dump, psql, pgbench, and other client tools

## Notes and Other Information
- The buffer system is designed to handle both text and binary data, though the terminating null character requirement makes it primarily suited for text operations
- Memory management follows standard malloc/free patterns with automatic expansion when needed
- The "broken" state mechanism provides graceful degradation when memory allocation fails
- This structure is typically used through the PQExpBuffer typedef (which is PQExpBufferData*) rather than directly
- Critical invariant: maxlen > len must be maintained except in error conditions
- Used throughout PostgreSQL's client-side utilities for SQL query construction, error message formatting, and general string manipulation tasks