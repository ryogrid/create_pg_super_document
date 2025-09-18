# int84

## Location
[src/backend/utils/adt/int8.c:1249-1261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1249-L1261)

## Overview
Converts a 64-bit integer (int8) to a 32-bit integer (int4) with overflow checking.

## Definition


## Detailed Description
The int84 function implements type conversion from PostgreSQL's 8-byte integer type (int8/bigint) to 4-byte integer type (int4/integer). This is a narrowing conversion that requires range checking since not all int8 values can be represented as int4. The function validates that the input value falls within the valid range for 32-bit signed integers (PG_INT32_MIN to PG_INT32_MAX) and throws an error if the value is out of range.

## Parameters / Member Variables
-  (int64): The 64-bit integer value to be converted to 32-bit

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting 64-bit integer argument)
  - PG_INT32_MIN (constant for minimum 32-bit integer value)
  - PG_INT32_MAX (constant for maximum 32-bit integer value)
  - ereport (function for reporting errors)
  - PG_RETURN_INT32 (macro for returning 32-bit integer result)
- Called from (representative examples):
  - [int8_to_char](int8_to_char.md) (in src/backend/utils/adt/formatting.c:6642)

## Notes and Other Information
- This function performs explicit range checking using PG_INT32_MIN and PG_INT32_MAX constants
- Throws a ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error with message "integer out of range" if conversion would cause overflow
- Uses unlikely() optimization hints to indicate that overflow conditions are rare
- Located in src/backend/utils/adt/int8.c in the conversion operators section  
- Essential for safe downcasting operations in PostgreSQL's type system
- The function name follows PostgreSQL's convention where the number indicates the byte size (8 for source, 4 for target)