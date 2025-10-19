# int82

## Location
[src/backend/utils/adt/int8.c:1270-1282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1270-L1282)

## Overview
Converts a 64-bit integer (int8) to a 16-bit integer (int2) with overflow checking.

## Definition

```c
Datum
int82(PG_FUNCTION_ARGS)
```
## Detailed Description
The int82 function implements type conversion from PostgreSQL's 8-byte integer type (int8/bigint) to 2-byte integer type (int2/smallint). This is a narrowing conversion that requires range checking since not all int8 values can be represented as int2. The function validates that the input value falls within the valid range for 16-bit signed integers (PG_INT16_MIN to PG_INT16_MAX) and throws an error if the value is out of range.

## Parameters / Member Variables
-  (int64): The 64-bit integer value to be converted to 16-bit

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting 64-bit integer argument)
  - PG_INT16_MIN (constant for minimum 16-bit integer value)  
  - PG_INT16_MAX (constant for maximum 16-bit integer value)
  - ereport (function for reporting errors)
  - PG_RETURN_INT16 (macro for returning 16-bit integer result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function performs explicit range checking using PG_INT16_MIN and PG_INT16_MAX constants
- Throws a ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error with message "smallint out of range" if conversion would cause overflow
- Uses unlikely() optimization hints to indicate that overflow conditions are rare
- Located in src/backend/utils/adt/int8.c in the conversion operators section
- Essential for safe downcasting operations in PostgreSQL's type system
- The function name follows PostgreSQL's convention where the number indicates the byte size (8 for source, 2 for target)
- Provides more restrictive range checking than int84, as smallint has a much smaller range than integer

## Simplified Source

```c
Datum int82(PG_FUNCTION_ARGS) {
    // Extract 64-bit integer argument
    int64 arg = PG_GETARG_INT64(0);

    // Check for overflow before conversion
    if (arg < PG_INT16_MIN || arg > PG_INT16_MAX) {
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("smallint out of range")));
    }

    // Safe to convert to 16-bit integer
    PG_RETURN_INT16((int16) arg);
}
```