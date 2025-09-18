# int8inc_float8_float8

## Location
[src/backend/utils/adt/int8.c:810-815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L810-L815)

## Overview
A specialized wrapper function for int8inc designed for aggregate operations involving float8 data types.

## Definition
```c
Datum int8inc_float8_float8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is another wrapper around int8inc, specifically designed for aggregate operations that work with float8 (double precision) data types. Despite the name suggesting float8 parameters, the function simply delegates to int8inc, indicating it's likely used in aggregate contexts where the counting mechanism (int8 increment) is the same regardless of the data type being aggregated.

The function name suggests it's used for operations that involve two float8 parameters but require int8 counting functionality, such as counting pairs of float8 values or similar aggregate operations.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention:
  - Arguments are passed through directly to int8inc without modification or type checking

## Dependencies
- Functions called/Symbols referenced:
  - [int8inc](int8inc.md) (the underlying increment function)
- Called from (representative examples):
  - No direct references found in codebase (likely used through PostgreSQL's aggregate system)

## Notes and Other Information
- Despite the name suggesting float8 parameters, it simply calls int8inc
- Used specifically for aggregate operations involving float8 data types
- Maintains separate function identity for proper aggregate system integration
- Part of PostgreSQL's aggregate function infrastructure for type-specific operations
- Located in src/backend/utils/adt/int8.c:810-815