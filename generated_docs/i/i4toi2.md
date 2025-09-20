# i4toi2

## Location
[src/backend/utils/adt/int.c:348-361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L348-L361)

## Overview
Converts a 32-bit integer (int4/integer) to a 16-bit integer (int2/smallint) with overflow checking in PostgreSQL.

## Definition
```c
Datum i4toi2(PG_FUNCTION_ARGS)
```

## Detailed Description
The i4toi2 function is a PostgreSQL type conversion function that narrows a 32-bit signed integer (int4/integer) to a 16-bit signed integer (int2/smallint). This is a potentially unsafe narrowing conversion that can result in data loss if the input value exceeds the range of a 16-bit integer. The function includes overflow checking to ensure the input value falls within the valid range for smallint (SHRT_MIN to SHRT_MAX), and raises an error if the value is out of range. This safety mechanism prevents silent data corruption during type conversions.

## Parameters / Member Variables
- Input: 32-bit signed integer retrieved via PG_GETARG_INT32(0)
- Output: Datum containing the converted 16-bit integer

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_INT16
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is a narrowing conversion that requires overflow checking
- Raises ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error if input is outside smallint range
- Uses unlikely() hints to optimize for the common case where conversion succeeds
- Part of PostgreSQL's integer type conversion routines
- The function follows PostgreSQL's fmgr (function manager) calling convention
- Located in src/backend/utils/adt/int.c:348-361
- Counterpart conversion function is i2toi4 (widening conversion)
- Used in explicit casts from integer to smallint