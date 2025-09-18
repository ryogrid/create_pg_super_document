# big5_to_utf8

## Location
src/backend/utils/mb/conversion_procs/utf8_and_big5/utf8_and_big5.c: 39 - 59

## Overview
Converts a string from BIG5 (Traditional Chinese) encoding to UTF-8 encoding as a PostgreSQL conversion function.

## Definition
```c
Datum big5_to_utf8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL conversion procedure that converts text strings from BIG5 encoding to UTF-8 encoding. BIG5 is a character encoding method used in Taiwan and Hong Kong for Traditional Chinese characters. The function is implemented as a PostgreSQL server function that can be called through the database's encoding conversion system.

The function uses the `LocalToUtf` utility function along with the `big5_to_unicode_tree` mapping table to perform the actual character conversion. It follows PostgreSQL's standard conversion function interface, accepting source and destination buffers, length information, and error handling preferences.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Source encoding ID (INTEGER) - should be PG_BIG5
- `PG_FUNCTION_ARGS[1]`: Destination encoding ID (INTEGER) - should be PG_UTF8  
- `PG_FUNCTION_ARGS[2]`: Source string (CSTRING) - null-terminated BIG5 encoded string
- `PG_FUNCTION_ARGS[3]`: Destination buffer (CSTRING) - buffer to store UTF-8 result
- `PG_FUNCTION_ARGS[4]`: Source string length (INTEGER) - length of source string in bytes
- `PG_FUNCTION_ARGS[5]`: Error handling flag (BOOL) - if true, don't throw error on conversion failure

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32  
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - [LocalToUtf](../L/LocalToUtf.md)
  - PG_RETURN_INT32
  - big5_to_unicode_tree (conversion mapping table)
- Called from (representative examples):
  - No direct references found (called through PostgreSQL's conversion system)

## Notes and Other Information
- Returns the number of bytes successfully converted as an INTEGER
- Uses the big5_to_unicode_tree mapping table defined in "../../Unicode/big5_to_utf8.map"  
- Part of PostgreSQL's multi-byte character set conversion system
- Registered as a PostgreSQL function via PG_FUNCTION_INFO_V1 macro
- Located in src/backend/utils/mb/conversion_procs/utf8_and_big5/utf8_and_big5.c:39-57