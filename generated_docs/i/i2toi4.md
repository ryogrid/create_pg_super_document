# i2toi4

## Location
[src/backend/utils/adt/int.c:340-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L340-L347)

## Overview
Converts a 16-bit integer (int2/smallint) to a 32-bit integer (int4/integer) in PostgreSQL.

## Definition
```c
Datum i2toi4(PG_FUNCTION_ARGS)
```

## Detailed Description
The i2toi4 function is a PostgreSQL type conversion function that promotes a 16-bit signed integer (int2/smallint) to a 32-bit signed integer (int4/integer). This is a widening conversion that preserves the original value without any data loss. The function is part of PostgreSQL's type conversion system and enables automatic or explicit casting between these integer types. Since this is a safe conversion (no overflow possible), the implementation is straightforward with simple type casting.

## Parameters / Member Variables
- No explicit parameters (uses PG_FUNCTION_ARGS macro)
- Input: 16-bit signed integer retrieved via PG_GETARG_INT16(0)
- Output: Datum containing the converted 32-bit integer

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is a safe widening conversion with no possibility of overflow
- Part of PostgreSQL's integer type conversion routines
- The function follows PostgreSQL's fmgr (function manager) calling convention
- Located in src/backend/utils/adt/int.c:340-347
- Counterpart conversion function is i4toi2 (narrowing conversion)
- Used in implicit and explicit casts from smallint to integer