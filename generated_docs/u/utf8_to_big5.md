# utf8_to_big5

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_big5/utf8_and_big5.c:60-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_big5/utf8_and_big5.c#L60-L78)

## Overview
Converts a string from UTF-8 encoding to BIG5 (Traditional Chinese) encoding as a PostgreSQL conversion function.

## Definition
```c
Datum utf8_to_big5(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL conversion procedure that converts text strings from UTF-8 encoding to BIG5 encoding. This is the reverse operation of `big5_to_utf8`, allowing conversion from the Unicode-based UTF-8 format back to the BIG5 encoding system used for Traditional Chinese characters in Taiwan and Hong Kong.

The function uses the `UtfToLocal` utility function along with the `big5_from_unicode_tree` mapping table to perform the actual character conversion. It follows PostgreSQL's standard conversion function interface, accepting source and destination buffers, length information, and error handling preferences.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Source encoding ID (INTEGER) - should be PG_UTF8
- `PG_FUNCTION_ARGS[1]`: Destination encoding ID (INTEGER) - should be PG_BIG5
- `PG_FUNCTION_ARGS[2]`: Source string (CSTRING) - null-terminated UTF-8 encoded string
- `PG_FUNCTION_ARGS[3]`: Destination buffer (CSTRING) - buffer to store BIG5 result  
- `PG_FUNCTION_ARGS[4]`: Source string length (INTEGER) - length of source string in bytes
- `PG_FUNCTION_ARGS[5]`: Error handling flag (BOOL) - if true, don't throw error on conversion failure

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - [UtfToLocal](../U/UtfToLocal.md)
  - PG_RETURN_INT32
  - big5_from_unicode_tree (conversion mapping table)
- Called from (representative examples):
  - No direct references found (called through PostgreSQL's conversion system)

## Notes and Other Information
- Returns the number of bytes successfully converted as an INTEGER
- Uses the big5_from_unicode_tree mapping table defined in "../../Unicode/utf8_to_big5.map"
- Part of PostgreSQL's multi-byte character set conversion system
- Registered as a PostgreSQL function via PG_FUNCTION_INFO_V1 macro
- Complementary function to `big5_to_utf8` for bidirectional encoding conversion
- Located in src/backend/utils/mb/conversion_procs/utf8_and_big5/utf8_and_big5.c:60-78

## Simplified Source

```c
Datum
utf8_to_big5(PG_FUNCTION_ARGS)
{
    // Extract function parameters
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
    int len = PG_GETARG_INT32(4);
    bool noError = PG_GETARG_BOOL(5);

    // Validate encoding conversion arguments
    CHECK_ENCODING_CONVERSION_ARGS(PG_UTF8, PG_BIG5);

    // Convert UTF-8 to BIG5 using conversion tree
    int converted = UtfToLocal(src, len, dest,
                              &big5_from_unicode_tree,
                              NULL, 0, NULL,
                              PG_BIG5, noError);

    return converted;
}
```