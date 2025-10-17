# utf8_to_euc_tw

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_euc_tw/utf8_and_euc_tw.c:60-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_euc_tw/utf8_and_euc_tw.c#L60-L78)

## Overview
Converts character encoding from UTF-8 to EUC-TW (Extended Unix Code for Taiwan), serving as a PostgreSQL conversion procedure function.

## Definition
```c
Datum utf8_to_euc_tw(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a PostgreSQL conversion procedure that transforms text encoded in UTF-8 format to EUC-TW encoding. It follows the standard PostgreSQL conversion procedure interface, accepting source and destination buffers along with conversion parameters. The function uses the UtfToLocal conversion utility with the EUC-TW from Unicode mapping tree to perform the actual character encoding transformation. It validates the encoding parameters and handles error conditions based on the noError flag.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `src` (unsigned char*): Source string in UTF-8 encoding (extracted from arg 2)
  - `dest` (unsigned char*): Destination buffer for EUC-TW output (extracted from arg 3) 
  - `len` (int): Length of source string in bytes (extracted from arg 4)
  - `noError` (bool): If true, don't throw an error if conversion fails (extracted from arg 5)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - [UtfToLocal](../U/UtfToLocal.md)
  - PG_RETURN_INT32
  - PG_UTF8 (encoding constant)
  - PG_EUC_TW (encoding constant)
  - euc_tw_from_unicode_tree (conversion mapping)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's encoding conversion system)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/utf8_and_euc_tw/utf8_and_euc_tw.c:60-78
- Returns the number of bytes successfully converted as an integer
- Part of PostgreSQL's multibyte character encoding conversion infrastructure
- Uses the euc_tw_from_unicode_tree mapping for character conversion
- Follows PostgreSQL's function calling conventions with PG_FUNCTION_ARGS interface
- EUC-TW is primarily used for Traditional Chinese text encoding in Taiwan
- Complementary function to euc_tw_to_utf8 for bidirectional encoding conversion

## Simplified Source
```c
Datum utf8_to_euc_tw(PG_FUNCTION_ARGS) {
    // Extract function arguments
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
    int len = PG_GETARG_INT32(4);
    bool noError = PG_GETARG_BOOL(5);

    // Validate encoding conversion arguments
    CHECK_ENCODING_CONVERSION_ARGS(PG_UTF8, PG_EUC_TW);

    // Perform UTF-8 to EUC-TW conversion using mapping tree
    int converted = UtfToLocal(src, len, dest,
                              &euc_tw_from_unicode_tree,
                              NULL, 0, NULL,
                              PG_EUC_TW, noError);

    return converted;
}
```