# pgwin32_message_to_UTF16

## Location
[src/backend/utils/mb/mbutils.c:1774-1837](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1774-L1837)

## Overview
Converts a string from PostgreSQL's message encoding to a null-terminated UTF-16 wide character string, primarily used for Windows event log and console output.

## Definition
```c
WCHAR *pgwin32_message_to_UTF16(const char *str, int len, int *utf16len)
```
Location: `src/backend/utils/mb/mbutils.c:1774-1837`

## Detailed Description
This function converts a string from PostgreSQL's current message encoding to UTF-16 format, which is required for Windows APIs like event logging and console output. The function handles two conversion paths:

1. **Direct conversion**: When the message encoding has a corresponding Windows codepage, it uses `MultiByteToWideChar` directly for optimal performance.

2. **Double conversion**: When no direct codepage mapping exists, it first converts from the message encoding to UTF-8 using PostgreSQL's encoding conversion system, then converts from UTF-8 to UTF-16.

The function includes special handling for transaction contexts, as PostgreSQL's encoding conversion functions require an active transaction. When no transaction is available, it assumes the input is already valid UTF-8.

Returns NULL if the message encoding is SQL_ASCII (no conversion possible) or if the conversion fails.

## Parameters / Member Variables
- `str`: Input string to convert from message encoding
- `len`: Length of the input string in bytes
- `utf16len`: Optional output parameter to receive the length of the resulting UTF-16 string in wide characters (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [GetMessageEncoding](../G/GetMessageEncoding.md): Gets the current message encoding ID
  - `PG_SQL_ASCII`: Constant for ASCII encoding (conversion not supported)
  - [IsTransactionState](../I/IsTransactionState.md): Checks if a database transaction is currently active
  - [pg_do_encoding_conversion](pg_do_encoding_conversion.md): Converts between different character encodings
  - `PG_UTF8`: Constant for UTF-8 encoding

- Called from (representative examples):
  - [write_eventlog](../w/write_eventlog.md) (src/backend/utils/error/elog.c:2545): For writing log messages to Windows Event Log
  - [write_console](../w/write_console.md) (src/backend/utils/error/elog.c:2606): For writing messages to Windows console

## Notes and Other Information
- This function is Windows-specific and part of PostgreSQL's Windows platform abstraction layer
- Memory is allocated using `palloc()` and must be freed by the caller using `pfree()`
- Before message encoding initialization, input should be ASCII-only, and the function behaves as if the message encoding is UTF-8
- The function gracefully handles cases where no transaction is active by assuming UTF-8 input
- Conversion failures result in NULL return value, allowing calling code to handle errors appropriately
- The UTF-16 output is null-terminated for compatibility with Windows string APIs