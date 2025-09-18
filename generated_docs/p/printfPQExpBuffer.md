# printfPQExpBuffer

## Location
src/interfaces/libpq/pqexpbuffer.c: 235 - 264

## Overview
Formats text data using printf-style formatting and replaces the entire contents of a PQExpBuffer with the formatted result.

## Definition
```c
void printfPQExpBuffer(PQExpBuffer str, const char *fmt, ...)
```

## Detailed Description
The `printfPQExpBuffer` function provides a convenient way to format text data using printf-style format strings and replace the entire contents of a PQExpBuffer. It essentially combines the functionality of `resetPQExpBuffer` followed by `appendPQExpBuffer`.

The function:
1. **Resets the buffer**: Calls `resetPQExpBuffer` to clear existing contents
2. **Validates state**: Checks if the buffer is in a broken state and returns early if so
3. **Formats and appends**: Uses a retry loop with `appendPQExpBufferVA` to handle potential buffer enlargement needs
4. **Preserves errno**: Saves and restores the errno value to avoid side effects

The retry loop is necessary because `appendPQExpBufferVA` may need to enlarge the buffer, and the formatting operation might need to be retried with the larger buffer.

## Parameters / Member Variables
- `str`: Pointer to the PQExpBuffer structure to format into
- `fmt`: Printf-style format string
- `...`: Variable arguments corresponding to the format string

## Dependencies
- Functions called/Symbols referenced:
  - resetPQExpBuffer
  - PQExpBufferBroken
  - appendPQExpBufferVA
- Called from (representative examples):
  - dumpTableData (pg_dump)
  - buildACLCommands (pg_dump utilities)
  - describeOneTableDetails (psql describe functions)
  - PQchangePassword (libpq authentication)
  - various PostgreSQL client tools for SQL generation

## Notes and Other Information
- This is a convenience function that combines reset and append operations
- Uses variable arguments (variadic function) similar to printf
- Automatically handles buffer enlargement through the retry mechanism
- Preserves errno to avoid interfering with error handling in calling code
- Extensively used throughout PostgreSQL client tools for SQL query construction and formatting
- Part of the libpq expandable string buffer interface
- The function is void - errors are indicated through the buffer's broken state