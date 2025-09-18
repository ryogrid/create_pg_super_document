# utf8_to_gbk

## Location
src/backend/utils/mb/conversion_procs/utf8_and_gbk/utf8_and_gbk.c: 60 - 78

## Overview
Converts a UTF-8 encoded string to GBK encoding as part of PostgreSQL's multibyte character conversion system.

## Definition
Datum utf8_to_gbk(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms text from UTF-8 encoding to GBK (Guojia Biaozhun, a Chinese character encoding). It follows the standard PostgreSQL conversion function interface pattern, accepting source and destination buffers along with conversion parameters. The function uses a radix tree lookup table (gbk_from_unicode_tree) for efficient character mapping and delegates the actual conversion work to the UtfToLocal utility function.

The function performs encoding validation to ensure the conversion is between the expected source (PG_UTF8) and destination (PG_GBK) encodings before proceeding with the conversion. This is the reverse operation of gbk_to_utf8.

## Parameters / Member Variables
- Source encoding ID (PG_GETARG parameter 0): Integer identifier for the source encoding (expected to be PG_UTF8)
- Destination encoding ID (PG_GETARG parameter 1): Integer identifier for the destination encoding (expected to be PG_GBK)
-  (PG_GETARG parameter 2): Pointer to the source UTF-8 encoded null-terminated C string
-  (PG_GETARG parameter 3): Pointer to the destination buffer for the GBK encoded result
-  (PG_GETARG parameter 4): Length of the source string in bytes
-  (PG_GETARG parameter 5): Boolean flag indicating whether to suppress errors on conversion failures

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for retrieving string arguments)
  - PG_GETARG_INT32 (macro for retrieving integer arguments)
  - PG_GETARG_BOOL (macro for retrieving boolean arguments)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [UtfToLocal](../U/UtfToLocal.md) (core conversion function)
  - PG_RETURN_INT32 (macro for returning integer results)
  - gbk_from_unicode_tree (radix tree data structure from utf8_to_gbk.map)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's conversion function registry)

## Notes and Other Information
- This function is registered as a PostgreSQL conversion procedure using PG_FUNCTION_INFO_V1 macro
- The conversion uses a pre-computed radix tree structure loaded from utf8_to_gbk.map for efficient Unicode-to-GBK character lookups
- Returns the number of bytes successfully converted as an integer
- Part of the UTF8_AND_GBK conversion module located in src/backend/utils/mb/conversion_procs/utf8_and_gbk/
- The function handles conversion errors gracefully when noError is set to true
- This provides the reverse conversion of gbk_to_utf8, allowing bidirectional text conversion between UTF-8 and GBK encodings
- Essential for Chinese text processing in PostgreSQL databases where GBK encoding is required