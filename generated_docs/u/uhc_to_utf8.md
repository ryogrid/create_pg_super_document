# uhc_to_utf8

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_uhc/utf8_and_uhc.c:39-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_uhc/utf8_and_uhc.c#L39-L59)

## Overview
A PostgreSQL encoding conversion function that converts text from UHC (Unified Hangul Code) encoding to UTF-8 encoding.

## Definition


## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms character strings from UHC encoding to UTF-8 encoding. UHC is a Korean character encoding standard that extends the EUC-KR encoding to include additional Hangul syllables and symbols. The function follows PostgreSQL's standard conversion procedure interface, accepting source and destination buffers along with conversion parameters, and returns the number of bytes successfully converted.

The conversion is performed using PostgreSQL's internal LocalToUtf utility function with a UHC-to-Unicode mapping tree. The function includes proper encoding validation and error handling capabilities.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function arguments containing:
  - **src** (arg 2): Source string in UHC encoding (null-terminated C string)
  - **dest** (arg 3): Destination buffer for UTF-8 encoded result (null-terminated C string)  
  - **len** (arg 4): Length of the source string in bytes
  - **noError** (arg 5): Boolean flag - if true, don't throw an error if conversion fails

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for extracting string arguments)
  - PG_GETARG_INT32 (macro for extracting integer arguments)
  - PG_GETARG_BOOL (macro for extracting boolean arguments)
  - CHECK_ENCODING_CONVERSION_ARGS (validates encoding conversion arguments)
  - [LocalToUtf](../L/LocalToUtf.md) (core conversion function from local encoding to UTF-8)
  - PG_RETURN_INT32 (macro for returning integer result)
  - uhc_to_unicode_tree (mapping tree for UHC to Unicode conversion)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via PostgreSQL's conversion procedure registry)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/utf8_and_uhc/utf8_and_uhc.c:39-59
- Part of PostgreSQL's multibyte character encoding conversion system
- Uses the standard PostgreSQL conversion procedure interface pattern
- Validates that source encoding is PG_UHC and destination encoding is PG_UTF8
- Returns the number of bytes successfully converted
- Supports graceful error handling when noError flag is set to true
- Works in conjunction with utf8_to_uhc for bidirectional UHC<->UTF-8 conversion