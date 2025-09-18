# latin2_to_win1250

## Location
src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c: 149 - 165

## Overview
A PostgreSQL character encoding conversion function that directly converts text from ISO 8859-2 (Latin-2) encoding to Windows-1250 encoding using a translation table.

## Definition
```c
Datum latin2_to_win1250(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL function interface for converting character data directly from ISO 8859-2 (Latin-2) encoding to Windows-1250 encoding. Unlike the MIC-based conversions, this function performs a direct single-byte charset conversion using the `local2local` utility function with the `iso88592_2_win1250` translation table. This provides an efficient direct conversion path between these two related character encodings without needing to go through the intermediate MIC format.

## Parameters / Member Variables
The function uses PostgreSQL's standard function argument interface:
- Argument 2: `src` - Source string in Latin-2 encoding to be converted
- Argument 3: `dest` - Destination buffer for the converted Windows-1250 output
- Argument 4: `len` - Length of the source string in bytes
- Argument 5: `noError` - Boolean flag indicating whether to suppress errors on invalid characters

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for extracting string arguments)
  - PG_GETARG_INT32 (macro for extracting integer arguments)
  - PG_GETARG_BOOL (macro for extracting boolean arguments)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - local2local (direct local charset conversion implementation)
  - PG_RETURN_INT32 (macro for returning integer values)
- Constants referenced:
  - PG_LATIN2 (Latin-2 encoding identifier)
  - PG_WIN1250 (Windows-1250 encoding identifier)
  - iso88592_2_win1250 (conversion table mapping ISO 8859-2 to Windows-1250)

## Notes and Other Information
- This function is typically registered as a conversion function in PostgreSQL's encoding conversion system
- The actual conversion logic uses the `local2local` utility function from `src/backend/utils/mb/conv.c`
- Located in: `src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c:149-165`
- Provides a direct conversion path between Latin-2 and Windows-1250 without MIC intermediary
- The conversion table `iso88592_2_win1250` maps characters from 0x80 to 0xFF, with 0x00 entries for untranslatable characters
- Part of PostgreSQL's multibyte character encoding conversion infrastructure
- Returns the number of input bytes successfully converted
- More efficient than going through MIC conversion for direct Latin-2 to Windows-1250 transformations
- Both encodings are ASCII-superset single-byte character sets, making direct conversion feasible