# utf8_to_shift_jis_2004

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_sjis2004/utf8_and_sjis2004.c:60-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_sjis2004/utf8_and_sjis2004.c#L60-L78)

## Overview
Converts text from UTF-8 encoding to Shift JIS 2004 encoding as a PostgreSQL conversion function.

## Definition
```c
Datum utf8_to_shift_jis_2004(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL encoding conversion procedure that converts text from UTF-8 to Shift JIS 2004 (Japanese character encoding). It follows the standard PostgreSQL conversion function interface, accepting source and destination buffers along with conversion parameters. The function uses the UtfToLocal conversion utility with Shift JIS 2004-specific mapping tables to perform the actual character conversion. The conversion process validates encoding parameters and can optionally suppress errors during conversion failures.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that expands to:
  - `src`: Source string in UTF-8 encoding (CSTRING argument 2)
  - `dest`: Destination buffer for Shift JIS 2004 output (CSTRING argument 3) 
  - `len`: Length of source string in bytes (INTEGER argument 4)
  - `noError`: Boolean flag to suppress conversion errors (BOOL argument 5)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING`: Extracts C string arguments from PostgreSQL function call
  - `PG_GETARG_INT32`: Extracts 32-bit integer arguments
  - `PG_GETARG_BOOL`: Extracts boolean arguments
  - `CHECK_ENCODING_CONVERSION_ARGS`: Validates source and destination encodings
  - [UtfToLocal](../U/UtfToLocal.md): Core conversion function for UTF-8 to local encoding
  - `PG_RETURN_INT32`: Returns 32-bit integer result
  - `shift_jis_2004_from_unicode_tree`: Conversion mapping tree from Unicode to Shift JIS 2004
  - `ULmapSHIFT_JIS_2004_combined`: Combined Unicode to local character mapping table
- Called from (representative examples):
  - No direct references found (likely registered as conversion procedure)

## Notes and Other Information
- This function is part of PostgreSQL's multi-byte character encoding conversion system
- Performs the reverse conversion of shift_jis_2004_to_utf8
- Shift JIS 2004 is an updated version of Shift JIS that includes additional kanji characters
- The function returns the number of bytes successfully converted
- Error handling can be controlled via the noError parameter
- Uses specialized mapping tables for accurate character conversion between encodings
- May encounter conversion failures when UTF-8 characters cannot be represented in Shift JIS 2004

## Simplified Source
```c
Datum
utf8_to_shift_jis_2004(PG_FUNCTION_ARGS)
{
    // Extract function arguments
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
    int len = PG_GETARG_INT32(4);
    bool noError = PG_GETARG_BOOL(5);

    // Validate encoding conversion arguments
    CHECK_ENCODING_CONVERSION_ARGS(PG_UTF8, PG_SHIFT_JIS_2004);

    // Convert using radix tree mapping with combined tables
    int converted = UtfToLocal(src, len, dest,
                              &shift_jis_2004_from_unicode_tree,
                              ULmapSHIFT_JIS_2004_combined,
                              lengthof(ULmapSHIFT_JIS_2004_combined),
                              NULL, PG_SHIFT_JIS_2004, noError);

    PG_RETURN_INT32(converted);
}
```