# utf8_to_gb18030

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c:215-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c#L215-L233)

## Overview
PostgreSQL conversion function that converts UTF-8 encoded text to GB18030 encoding, a Chinese character encoding standard.

## Definition


## Detailed Description
The `utf8_to_gb18030` function is a PostgreSQL conversion procedure that transforms UTF-8 encoded text into GB18030 encoding. GB18030 is a Chinese national standard character encoding that is backward compatible with GBK and GB2312, capable of representing all Unicode code points.

This function serves as a PostgreSQL function wrapper around the core conversion logic implemented in `UtfToLocal`. It extracts the function arguments using PostgreSQL's function calling convention, validates the encoding parameters, and delegates the actual conversion work to the generic UTF-8 to local encoding conversion function.

The conversion process utilizes a radix tree (`gb18030_from_unicode_tree`) for efficient character mapping lookups and an algorithmic conversion function (`conv_utf8_to_18030`) for characters that require computational conversion rather than direct table lookup.

## Parameters / Member Variables
- `src`: Source string in UTF-8 encoding (input parameter 2)
- `dest`: Destination buffer for GB18030 encoded output (input parameter 3)
- `len`: Length of the source string in bytes (input parameter 4)
- `noError`: Boolean flag indicating whether to suppress conversion errors (input parameter 5)
- `converted`: Number of input bytes successfully converted (return value)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING`
  - `PG_GETARG_INT32`
  - `PG_GETARG_BOOL`
  - `CHECK_ENCODING_CONVERSION_ARGS`
  - [UtfToLocal](../U/UtfToLocal.md)
  - [conv_utf8_to_18030](../c/conv_utf8_to_18030.md)
  - `PG_RETURN_INT32`
- Data structures referenced:
  - `gb18030_from_unicode_tree`
  - `PG_UTF8`
  - `PG_GB18030`
- Called from:
  - Not directly referenced by other functions (likely called through PostgreSQL's function call mechanism)

## Notes and Other Information
- This is a PostgreSQL V1 calling convention function, indicated by the `PG_FUNCTION_INFO_V1` declaration
- The function validates that the source encoding is UTF-8 and target encoding is GB18030 using `CHECK_ENCODING_CONVERSION_ARGS`
- Character conversion is performed using a three-tier approach: combined character map lookup, single character radix tree lookup, and algorithmic conversion
- The `noError` parameter allows for partial conversions when encountering untranslatable characters
- The function returns the number of input bytes consumed, which may be less than the input length if conversion errors occur and `noError` is true
- Located in: src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c:215-233