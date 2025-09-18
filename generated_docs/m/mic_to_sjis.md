# mic_to_sjis

## Location
src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c: 141 - 159

## Overview
PostgreSQL function that provides a conversion interface from Mule Internal Code (MIC) to Shift JIS encoding for Japanese character sets.

## Definition


## Detailed Description
This function serves as a PostgreSQL function wrapper for the mic2sjis conversion routine. It extracts the necessary parameters from the PostgreSQL function call interface and delegates the actual character encoding conversion to the mic2sjis function. The function validates the encoding conversion arguments and returns the number of converted bytes.

## Parameters / Member Variables
The function uses PostgreSQL's PG_FUNCTION_ARGS macro to access parameters:
-  (PG_GETARG_CSTRING(2)): Source string in Mule Internal Code encoding
-  (PG_GETARG_CSTRING(3)): Destination buffer for Shift JIS encoded output
-  (PG_GETARG_INT32(4)): Length of the source string
-  (PG_GETARG_BOOL(5)): Boolean flag indicating whether to suppress errors during conversion

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING: Extract string arguments from PostgreSQL function call
  - PG_GETARG_INT32: Extract integer argument from PostgreSQL function call
  - PG_GETARG_BOOL: Extract boolean argument from PostgreSQL function call
  - CHECK_ENCODING_CONVERSION_ARGS: Validate encoding conversion parameters
  - mic2sjis: Perform the actual MIC to Shift JIS conversion
  - PG_RETURN_INT32: Return integer result to PostgreSQL
- Called from (representative examples):
  - PGEUCALTCODE: Referenced in the encoding conversion system

## Notes and Other Information
- This function is part of PostgreSQL's multibyte character encoding conversion system
- It specifically handles conversion from PostgreSQL's internal Mule Internal Code to the Japanese Shift JIS encoding
- The function validates that the source encoding is PG_MULE_INTERNAL and target encoding is PG_SJIS
- Returns the number of bytes successfully converted
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:141-159