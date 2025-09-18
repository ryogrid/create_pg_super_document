# pq_writestring

## Location
src/include/libpq/pqformat.h: 108 - 127

## Overview
A static inline function that appends a null-terminated string to a StringInfo buffer with automatic character encoding conversion for PostgreSQL's libpq protocol format handling.

## Definition
```c
static inline void pq_writestring(StringInfoData *pg_restrict buf, const char *pg_restrict str)
```

## Detailed Description
The `pq_writestring` function is a string serialization utility that writes a null-terminated text string to a pre-allocated StringInfo buffer with automatic character encoding conversion from server encoding to client encoding. This function is essential for ensuring that string data transmitted over PostgreSQL's protocol is properly encoded for the client's expected character set.

The function performs several key operations: it calculates the string length, converts the string from server encoding to client encoding using `pg_server_to_client`, copies the converted string (including the null terminator) to the buffer, and manages memory cleanup for any temporary conversion buffers. The function assumes sufficient buffer space has been pre-allocated for the string after conversion.

## Parameters / Member Variables
- `buf`: A pointer to a StringInfoData structure representing the output buffer. Must have sufficient pre-allocated space for the string after encoding conversion.
- `str`: A null-terminated string in server encoding to be written to the buffer. The string will be converted to client encoding before writing.

## Dependencies
- Functions called/Symbols referenced:
  - pg_server_to_client (character encoding conversion function)
  - strlen (standard library function)
  - memcpy (standard library function)
  - pfree (PostgreSQL memory management function)
  - Assert (macro)
- Called from (representative examples):
  - SendRowDescriptionMessage

## Notes and Other Information
- Automatically handles character encoding conversion from server to client encoding
- The pre-allocated buffer space must account for potential size changes due to encoding conversion
- Includes null terminator in the transmitted data, maintaining string semantics in the protocol
- Properly manages memory by freeing temporary conversion buffers when needed
- Uses `pg_restrict` annotations for performance optimization
- Critical for internationalization support in PostgreSQL's protocol, ensuring proper character encoding handling across different locales and client configurations
- The function assumes the input string is valid and null-terminated, following PostgreSQL's string handling conventions