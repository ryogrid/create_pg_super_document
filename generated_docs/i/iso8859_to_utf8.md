# iso8859_to_utf8

## Location
src/backend/utils/mb/conversion_procs/utf8_and_iso8859/utf8_and_iso8859.c: 100 - 135

## Overview
Converts text from various ISO 8859 character encodings to UTF-8 encoding using PostgreSQL's conversion framework.

## Definition
```c
Datum iso8859_to_utf8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms text data from ISO 8859 character sets (Latin 2-10 and ISO 8859-5 through ISO 8859-8) to UTF-8 encoding. It operates by looking up the appropriate conversion mapping table for the source encoding and delegating the actual conversion work to the `LocalToUtf` function. The function supports multiple ISO 8859 variants through a static mapping table that associates encoding IDs with their corresponding conversion trees.

The function follows PostgreSQL's standard conversion procedure interface, accepting encoding IDs, source and destination buffers, length information, and error handling preferences. It validates that the target encoding is UTF-8 and ensures the source encoding corresponds to a supported ISO 8859 variant.

## Parameters / Member Variables
- `encoding` (PG_GETARG_INT32(0)): The source encoding ID, must correspond to one of the supported ISO 8859 variants
- `src` (PG_GETARG_CSTRING(2)): Pointer to the source string buffer containing data in the source encoding  
- `dest` (PG_GETARG_CSTRING(3)): Pointer to the destination buffer where UTF-8 encoded data will be written
- `len` (PG_GETARG_INT32(4)): Length of the source string in bytes
- `noError` (PG_GETARG_BOOL(5)): Boolean flag indicating whether to suppress conversion errors (true) or throw exceptions (false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32
  - PG_GETARG_CSTRING
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - lengthof
  - [LocalToUtf](../L/LocalToUtf.md)
  - PG_RETURN_INT32
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - No direct callers found (likely registered as conversion procedure)

## Notes and Other Information
- Supports ISO 8859-2 through ISO 8859-10 (Latin scripts) and ISO 8859-5 through ISO 8859-8
- Uses radix tree-based mapping tables for efficient character conversion
- Raises an ERRCODE_INTERNAL_ERROR if an unsupported encoding ID is provided
- The conversion mappings are defined in separate .map files included at compile time
- Returns the number of bytes successfully converted
- Part of PostgreSQL's pluggable character set conversion system