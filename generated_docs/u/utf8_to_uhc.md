# utf8_to_uhc

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_uhc/utf8_and_uhc.c:60-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_uhc/utf8_and_uhc.c#L60-L78)

## Overview
A PostgreSQL encoding conversion function that converts text from UTF-8 encoding to UHC (Unified Hangul Code) encoding.

## Definition


## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms character strings from UTF-8 encoding to UHC encoding. UHC (Unified Hangul Code) is a Korean character encoding standard that extends EUC-KR to support additional Hangul syllables and symbols. The function follows PostgreSQL's standard conversion procedure interface, accepting source and destination buffers along with conversion parameters, and returns the number of bytes successfully converted.

The conversion is performed using PostgreSQL's internal UtfToLocal utility function with a Unicode-to-UHC mapping tree. The function includes proper encoding validation and error handling capabilities, making it the complementary counterpart to uhc_to_utf8.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function arguments containing:
  - **src** (arg 2): Source string in UTF-8 encoding (null-terminated C string)
  - **dest** (arg 3): Destination buffer for UHC encoded result (null-terminated C string)
  - **len** (arg 4): Length of the source string in bytes
  - **noError** (arg 5): Boolean flag - if true, don't throw an error if conversion fails

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for extracting string arguments)
  - PG_GETARG_INT32 (macro for extracting integer arguments)
  - PG_GETARG_BOOL (macro for extracting boolean arguments)
  - CHECK_ENCODING_CONVERSION_ARGS (validates encoding conversion arguments)
  - [UtfToLocal](../U/UtfToLocal.md) (core conversion function from UTF-8 to local encoding)
  - PG_RETURN_INT32 (macro for returning integer result)
  - uhc_from_unicode_tree (mapping tree for Unicode to UHC conversion)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via PostgreSQL's conversion procedure registry)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/utf8_and_uhc/utf8_and_uhc.c:60-78
- Part of PostgreSQL's multibyte character encoding conversion system
- Uses the standard PostgreSQL conversion procedure interface pattern
- Validates that source encoding is PG_UTF8 and destination encoding is PG_UHC
- Returns the number of bytes successfully converted
- Supports graceful error handling when noError flag is set to true
- Works in conjunction with uhc_to_utf8 for bidirectional UHC<->UTF-8 conversion
- Essential for Korean language support in PostgreSQL databases