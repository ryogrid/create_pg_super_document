# euc_jis_2004_to_utf8

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_euc2004/utf8_and_euc2004.c:39-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_euc2004/utf8_and_euc2004.c#L39-L59)

## Overview
Converts text from EUC-JIS-2004 encoding to UTF-8 encoding, serving as a PostgreSQL conversion function for Japanese text processing.

## Definition

```c
Datum
euc_jis_2004_to_utf8(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements a PostgreSQL conversion procedure that transforms text encoded in EUC-JIS-2004 (Extended Unix Code for Japanese Industrial Standards 2004) to UTF-8 encoding. It follows the standard PostgreSQL conversion function signature, accepting source and destination buffers along with conversion parameters. The function utilizes the  conversion engine with EUC-JIS-2004 specific mapping tables and conversion trees to perform the character encoding transformation.

The function validates the encoding conversion arguments and delegates the actual conversion work to the  utility function, which handles the complex character-by-character mapping using specialized lookup tables and conversion trees optimized for EUC-JIS-2004 to Unicode transformation.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - Argument 2: Source string (CSTRING) - null-terminated C string in EUC-JIS-2004 encoding
  - Argument 3: Destination buffer (CSTRING) - null-terminated C string buffer for UTF-8 output
  - Argument 4: Source string length (INTEGER) - length of the input string in bytes
  - Argument 5: noError flag (BOOL) - if true, doesn't throw an error if conversion fails

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (argument extraction)
  - PG_GETARG_INT32 (argument extraction)
  - PG_GETARG_BOOL (argument extraction)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [LocalToUtf](../L/LocalToUtf.md) (core conversion function)
  - PG_RETURN_INT32 (return value macro)
  - lengthof (array length utility)
- Constants used:
  - PG_EUC_JIS_2004 (source encoding identifier)
  - PG_UTF8 (destination encoding identifier)
  - euc_jis_2004_to_unicode_tree (conversion tree structure)
  - LUmapEUC_JIS_2004_combined (mapping table array)
- Called from:
  - No direct references found (likely called via PostgreSQL's conversion function registry)

## Notes and Other Information
- This function is part of PostgreSQL's multibyte character encoding conversion system
- EUC-JIS-2004 is an encoding specifically designed for Japanese text that supports the JIS X 0213:2004 character set
- The conversion uses specialized lookup tables and tree structures optimized for EUC-JIS-2004 character mappings
- Returns the number of bytes successfully converted
- Located in src/backend/utils/mb/conversion_procs/utf8_and_euc2004/utf8_and_euc2004.c:39-59
- Follows PostgreSQL's standard conversion function protocol for encoding transformations