# int28

## Location
src/backend/utils/adt/int8.c: 1262 - 1269

## Overview
Converts a 16-bit integer (int2) to a 64-bit integer (int8) with sign extension.

## Definition


## Detailed Description
The int28 function implements type conversion from PostgreSQL's 2-byte integer type (int2/smallint) to 8-byte integer type (int8/bigint). This is a widening conversion that preserves the original value by extending the sign bit from 16 bits to 64 bits. The conversion is lossless, as all int2 values can be exactly represented as int8 values.

## Parameters / Member Variables
-  (int16): The 16-bit integer value to be converted to 64-bit

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (macro for extracting 16-bit integer argument)
  - PG_RETURN_INT64 (macro for returning 64-bit integer result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function is part of PostgreSQL's type conversion system, allowing implicit or explicit casting from smallint to bigint
- The conversion is performed using C's standard type casting, which handles sign extension automatically
- Located in src/backend/utils/adt/int8.c in the conversion operators section
- Commonly used in SQL operations where int2 values need to be promoted to int8 for arithmetic or comparison operations
- The function name follows PostgreSQL's convention where the number indicates the byte size (2 for source, 8 for target)
- No range checking is needed since all 16-bit values fit within the 64-bit integer range