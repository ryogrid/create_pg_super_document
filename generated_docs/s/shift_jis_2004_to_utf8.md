# shift_jis_2004_to_utf8

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_sjis2004/utf8_and_sjis2004.c:39-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_sjis2004/utf8_and_sjis2004.c#L39-L59)

## Overview
Converts text from Shift JIS 2004 encoding to UTF-8 encoding as a PostgreSQL conversion function.

## Definition


## Detailed Description
This function is a PostgreSQL encoding conversion procedure that converts text from Shift JIS 2004 (Japanese character encoding) to UTF-8 encoding. It follows the standard PostgreSQL conversion function interface, accepting source and destination buffers along with conversion parameters. The function uses the LocalToUtf conversion utility with Shift JIS 2004-specific mapping tables to perform the actual character conversion. The conversion process validates encoding parameters and can optionally suppress errors during conversion failures.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that expands to:
  - : Source string in Shift JIS 2004 encoding (CSTRING argument 2)
  - : Destination buffer for UTF-8 output (CSTRING argument 3) 
  - : Length of source string in bytes (INTEGER argument 4)
  - : Boolean flag to suppress conversion errors (BOOL argument 5)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts C string arguments from PostgreSQL function call
  - : Extracts 32-bit integer arguments
  - : Extracts boolean arguments
  - : Validates source and destination encodings
  - : Core conversion function for local encoding to UTF-8
  - : Returns 32-bit integer result
  - : Conversion mapping tree for Shift JIS 2004
  - : Combined character mapping table
- Called from (representative examples):
  - No direct references found (likely registered as conversion procedure)

## Notes and Other Information
- This function is part of PostgreSQL's multi-byte character encoding conversion system
- Shift JIS 2004 is an updated version of Shift JIS that includes additional kanji characters
- The function returns the number of bytes successfully converted
- Error handling can be controlled via the noError parameter
- Uses specialized mapping tables for accurate character conversion between encodings