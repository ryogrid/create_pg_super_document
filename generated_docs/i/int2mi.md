# int2mi

## Location
src/backend/utils/adt/int.c: 920 - 933

## Overview
A PostgreSQL system function that implements subtraction for 16-bit signed integers (int2/smallint) with overflow detection and error handling.

## Definition
```c
Datum int2mi(PG_FUNCTION_ARGS)
```

## Detailed Description
The int2mi function implements the binary subtraction operator (-) for PostgreSQL's int2 (smallint) data type. It takes two int16 values as input and returns their difference, with built-in overflow detection to prevent arithmetic underflow/overflow errors. If the subtraction would result in a value outside the valid range for int16 (-32768 to 32767), the function raises a PostgreSQL error with the message "smallint out of range".

The function uses PostgreSQL's safe arithmetic functions (pg_sub_s16_overflow) to detect overflow conditions before they occur, ensuring data integrity. This is particularly important for subtraction operations where subtracting a large negative number from a positive number, or vice versa, could cause overflow.

## Parameters / Member Variables
- `arg1`: First int16 operand (minuend) obtained via PG_GETARG_INT16(0)
- `arg2`: Second int16 operand (subtrahend) obtained via PG_GETARG_INT16(1)
- `result`: Local variable to store the subtraction result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (macro for extracting int16 arguments)
  - pg_sub_s16_overflow (safe subtraction with overflow detection)
  - ereport (error reporting function)
  - errcode/errmsg (error handling macros)
  - PG_RETURN_INT16 (macro for returning int16 value)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through operator dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:920-933
- This function is typically invoked through PostgreSQL's operator system when the - operator is used with two smallint values
- Throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error if overflow occurs
- Part of PostgreSQL's arithmetic operator family for the int2/smallint data type
- Uses the unlikely() macro hint to optimize for the common case where overflow does not occur
- Overflow can occur in subtraction when subtracting a large negative number from a positive number or subtracting a large positive number from a negative number