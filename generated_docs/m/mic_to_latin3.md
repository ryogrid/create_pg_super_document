# mic_to_latin3

## Location
[src/backend/utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c:91-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c#L91-L106)

## Overview
A PostgreSQL encoding conversion function that converts text from the Multi-byte Internal Code (MIC) encoding to Latin-3 (ISO 8859-3) encoding, performing the reverse conversion of latin3_to_mic.

## Definition


## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms character data from MIC (Multi-byte Internal Code) encoding back to Latin-3 encoding. It acts as a PostgreSQL function interface wrapper around the lower-level  conversion routine. The function follows PostgreSQL's standard conversion function protocol and specifically handles the conversion from PG_MULE_INTERNAL to PG_LATIN3 encoding types. This is the inverse operation of the latin3_to_mic function, converting back to Latin-3 encoding which supports South European languages including Turkish, Maltese, and Esperanto.

## Parameters / Member Variables
-  (src): Source string buffer containing MIC encoded text to be converted
-  (dest): Destination string buffer where Latin-3 encoded result will be stored
-  (len): Length of the source string in bytes
-  (noError): Boolean flag indicating whether to suppress errors on conversion failure

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - [mic2latin](mic2latin.md)
  - PG_RETURN_INT32
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- The function validates the encoding conversion arguments using CHECK_ENCODING_CONVERSION_ARGS macro
- Uses LC_ISO8859_3 locale constant when calling the underlying mic2latin function
- Returns the number of bytes successfully converted as an integer
- Part of PostgreSQL's character set conversion infrastructure
- Located in the latin_and_mic conversion module
- Follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS
- Performs the reverse operation of latin3_to_mic function
- Converts back to Latin-3 encoding which is designed for South European languages