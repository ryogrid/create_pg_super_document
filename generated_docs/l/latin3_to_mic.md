# latin3_to_mic

## Location
src/backend/utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c: 75 - 90

## Overview
A PostgreSQL encoding conversion function that converts text from Latin-3 (ISO 8859-3) encoding to the Multi-byte Internal Code (MIC) encoding used internally by PostgreSQL's multi-byte character system.

## Definition


## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms character data from Latin-3 encoding to MIC (Multi-byte Internal Code) encoding. It acts as a PostgreSQL function interface wrapper around the lower-level  conversion routine. The function follows PostgreSQL's standard conversion function protocol and specifically handles the conversion from PG_LATIN3 to PG_MULE_INTERNAL encoding types. Latin-3 (ISO 8859-3) is designed for South European languages including Turkish, Maltese, and Esperanto.

## Parameters / Member Variables
-  (src): Source string buffer containing Latin-3 encoded text to be converted
-  (dest): Destination string buffer where MIC encoded result will be stored
-  (len): Length of the source string in bytes
-  (noError): Boolean flag indicating whether to suppress errors on conversion failure

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - latin2mic
  - PG_RETURN_INT32
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- The function validates the encoding conversion arguments using CHECK_ENCODING_CONVERSION_ARGS macro
- Uses LC_ISO8859_3 locale constant when calling the underlying latin2mic function
- Returns the number of bytes successfully converted as an integer
- Part of PostgreSQL's character set conversion infrastructure
- Located in the latin_and_mic conversion module
- Follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS
- Latin-3 encoding supports characters for South European languages with special focus on Turkish, Maltese, and Esperanto