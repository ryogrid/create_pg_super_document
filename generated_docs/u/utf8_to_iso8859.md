# utf8_to_iso8859

## Location
src/backend/utils/mb/conversion_procs/utf8_and_iso8859/utf8_and_iso8859.c: 136 - 169

## Overview
Converts text from UTF-8 encoding to various ISO 8859 character encodings using PostgreSQL's conversion framework.

## Definition
```c
Datum utf8_to_iso8859(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms UTF-8 encoded text data to various ISO 8859 character sets (Latin 2-10 and ISO 8859-5 through ISO 8859-8). It operates by looking up the appropriate conversion mapping table for the target encoding and delegating the actual conversion work to the `UtfToLocal` function. The function supports multiple ISO 8859 variants through a static mapping table that associates encoding IDs with their corresponding conversion trees.

The function follows PostgreSQL's standard conversion procedure interface, accepting encoding IDs, source and destination buffers, length information, and error handling preferences. It validates that the source encoding is UTF-8 and ensures the target encoding corresponds to a supported ISO 8859 variant.

## Parameters / Member Variables
- `encoding` (PG_GETARG_INT32(1)): The target encoding ID, must correspond to one of the supported ISO 8859 variants
- `src` (PG_GETARG_CSTRING(2)): Pointer to the source string buffer containing UTF-8 encoded data
- `dest` (PG_GETARG_CSTRING(3)): Pointer to the destination buffer where data in the target encoding will be written  
- `len` (PG_GETARG_INT32(4)): Length of the source string in bytes
- `noError` (PG_GETARG_BOOL(5)): Boolean flag indicating whether to suppress conversion errors (true) or throw exceptions (false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32
  - PG_GETARG_CSTRING  
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - lengthof
  - [UtfToLocal](../U/UtfToLocal.md)
  - PG_RETURN_INT32
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - No direct callers found (likely registered as conversion procedure)

## Notes and Other Information
- Supports conversion to ISO 8859-2 through ISO 8859-10 (Latin scripts) and ISO 8859-5 through ISO 8859-8
- Uses radix tree-based mapping tables for efficient character conversion
- Raises an ERRCODE_INTERNAL_ERROR if an unsupported encoding ID is provided
- The conversion mappings are defined in separate .map files included at compile time
- Returns the number of bytes successfully converted
- Part of PostgreSQL's pluggable character set conversion system
- Note that UTF-8 can represent characters not present in ISO 8859 sets, which may cause conversion errors depending on the noError parameter