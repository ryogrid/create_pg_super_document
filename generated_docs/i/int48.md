# int48

## Location
[src/backend/utils/adt/int8.c:1241-1248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1241-L1248)

## Overview
Converts a 32-bit integer (int4) to a 64-bit integer (int8) with sign extension.

## Definition


## Detailed Description
The int48 function implements type conversion from PostgreSQL's 4-byte integer type (int4/integer) to 8-byte integer type (int8/bigint). This is a widening conversion that preserves the original value by extending the sign bit from 32 bits to 64 bits. The conversion is lossless, as all int4 values can be exactly represented as int8 values.

## Parameters / Member Variables
-  (int32): The 32-bit integer value to be converted to 64-bit

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro for extracting 32-bit integer argument)
  - PG_RETURN_INT64 (macro for returning 64-bit integer result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function is part of PostgreSQL's type conversion system, allowing implicit or explicit casting from integer to bigint
- The conversion is performed using C's standard type casting, which handles sign extension automatically
- Located in src/backend/utils/adt/int8.c in the conversion operators section
- Commonly used in SQL operations where int4 values need to be promoted to int8 for arithmetic or comparison operations
- The function name follows PostgreSQL's convention where the number indicates the byte size (4 for source, 8 for target)