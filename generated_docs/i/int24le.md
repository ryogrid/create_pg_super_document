# int24le

## Location
[src/backend/utils/adt/int.c:531-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L531-L539)

## Overview
Compares a 16-bit signed integer (int2) with a 32-bit signed integer (int4) for less-than-or-equal relationship, returning true if the first value is less than or equal to the second.

## Definition
```c
Datum int24le(PG_FUNCTION_ARGS)
```

## Detailed Description
The int24le function implements the less-than-or-equal comparison operator for mixed integer types in PostgreSQL's type system. It takes a 16-bit signed integer (smallint/int2) as the first argument and a 32-bit signed integer (integer/int4) as the second argument. The function performs a direct comparison between the two values after implicit type promotion, returning a boolean result indicating whether the first value is less than or equal to the second value.

This function is part of PostgreSQL's comprehensive set of cross-type comparison operators that enable seamless comparisons between different integer types without requiring explicit casting in SQL queries.

## Parameters / Member Variables
- `arg1`: 16-bit signed integer (int16/smallint) - the left operand of the comparison
- `arg2`: 32-bit signed integer (int32/integer) - the right operand of the comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (macro to extract int16 argument)
  - PG_GETARG_INT32 (macro to extract int32 argument)  
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:531-539
- The comparison leverages C's automatic type promotion where the int16 value is implicitly promoted to int32 for the comparison
- Part of PostgreSQL's operator system, typically invoked through SQL expressions like 'smallint_val <= integer_val'
- Returns boolean true when first value is less than or equal to second, false otherwise

## Simplified Source

```c
Datum int24le(PG_FUNCTION_ARGS) {
    // Extract int16 and int32 arguments
    int16 left_value = PG_GETARG_INT16(0);
    int32 right_value = PG_GETARG_INT32(1);

    // Compare for less-than-or-equal relationship (C automatically promotes int16 to int32)
    PG_RETURN_BOOL(left_value <= right_value);
}
```