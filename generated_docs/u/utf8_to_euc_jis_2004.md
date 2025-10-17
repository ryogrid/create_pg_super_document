# utf8_to_euc_jis_2004

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_euc2004/utf8_and_euc2004.c:60-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_euc2004/utf8_and_euc2004.c#L60-L78)

## Overview
Converts text from UTF-8 encoding to EUC-JIS-2004 encoding, serving as a PostgreSQL conversion function for Japanese text processing.

## Definition

```c
Datum
utf8_to_euc_jis_2004(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements a PostgreSQL conversion procedure that transforms text encoded in UTF-8 to EUC-JIS-2004 (Extended Unix Code for Japanese Industrial Standards 2004) encoding. It serves as the reverse conversion counterpart to euc_jis_2004_to_utf8, following the standard PostgreSQL conversion function signature with source and destination buffers and conversion parameters. The function utilizes the  conversion engine with EUC-JIS-2004 specific mapping tables and conversion trees to perform the character encoding transformation from Unicode to the Japanese encoding standard.

The function validates the encoding conversion arguments and delegates the actual conversion work to the  utility function, which handles the complex Unicode-to-local character mapping using specialized lookup tables and conversion trees optimized for Unicode to EUC-JIS-2004 transformation.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - Argument 2: Source string (CSTRING) - null-terminated C string in UTF-8 encoding
  - Argument 3: Destination buffer (CSTRING) - null-terminated C string buffer for EUC-JIS-2004 output
  - Argument 4: Source string length (INTEGER) - length of the input string in bytes
  - Argument 5: noError flag (BOOL) - if true, doesn't throw an error if conversion fails

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (argument extraction)
  - PG_GETARG_INT32 (argument extraction)
  - PG_GETARG_BOOL (argument extraction)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [UtfToLocal](../U/UtfToLocal.md) (core conversion function)
  - PG_RETURN_INT32 (return value macro)
  - lengthof (array length utility)
- Constants used:
  - PG_UTF8 (source encoding identifier)
  - PG_EUC_JIS_2004 (destination encoding identifier)
  - euc_jis_2004_from_unicode_tree (conversion tree structure)
  - ULmapEUC_JIS_2004_combined (mapping table array)
- Called from:
  - No direct references found (likely called via PostgreSQL's conversion function registry)

## Notes and Other Information
- This function is part of PostgreSQL's multibyte character encoding conversion system
- Performs the reverse conversion of euc_jis_2004_to_utf8, converting from UTF-8 to EUC-JIS-2004
- EUC-JIS-2004 is an encoding specifically designed for Japanese text that supports the JIS X 0213:2004 character set
- The conversion uses specialized lookup tables and tree structures optimized for Unicode to EUC-JIS-2004 character mappings
- Returns the number of bytes successfully converted
- Located in src/backend/utils/mb/conversion_procs/utf8_and_euc2004/utf8_and_euc2004.c:60-78
- Follows PostgreSQL's standard conversion function protocol for encoding transformations
- Uses  and  for reverse mapping from Unicode to EUC-JIS-2004

## Simplified Source

```c
Datum
utf8_to_euc_jis_2004(PG_FUNCTION_ARGS)
{
    // Extract function parameters
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
    int len = PG_GETARG_INT32(4);
    bool noError = PG_GETARG_BOOL(5);

    // Validate encoding conversion arguments
    CHECK_ENCODING_CONVERSION_ARGS(PG_UTF8, PG_EUC_JIS_2004);

    // Convert UTF-8 to EUC-JIS-2004 using conversion tree and combined mapping
    int converted = UtfToLocal(src, len, dest,
                              &euc_jis_2004_from_unicode_tree,
                              ULmapEUC_JIS_2004_combined,
                              lengthof(ULmapEUC_JIS_2004_combined),
                              NULL, PG_EUC_JIS_2004, noError);

    return converted;
}
```