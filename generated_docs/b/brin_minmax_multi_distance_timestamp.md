# brin_minmax_multi_distance_timestamp

## Location
[src/backend/access/brin/brin_minmax_multi.c:2137-2154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2137-L2154)

## Overview
Computes the distance between two timestamp values as a floating-point number, used by BRIN (Block Range Index) minmax multi operator classes for timestamp data types.

## Definition
```c
Datum brin_minmax_multi_distance_timestamp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the numerical distance between two timestamp values by converting them to float8 and computing their difference. The function is part of PostgreSQL's BRIN minmax multi operator class infrastructure, which supports more efficient indexing of timestamp columns by maintaining multiple min/max pairs per block range. The distance calculation is essential for determining the "spread" of timestamp values within a block range, helping the optimizer make better decisions about index usage.

## Parameters / Member Variables
- `PG_GETARG_TIMESTAMP(0)`: The first timestamp value (dt1)  
- `PG_GETARG_TIMESTAMP(1)`: The second timestamp value (dt2)
- Returns: `float8` representing the distance between the two timestamps

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP (macro for extracting timestamp arguments)
  - PG_RETURN_FLOAT8 (macro for returning float8 result)
  - Timestamp (PostgreSQL timestamp data type)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- The function assumes dt2 >= dt1 and includes an Assert to verify this condition
- The distance is computed as a simple arithmetic difference after casting timestamps to float8
- This function is typically registered in the operator class definition for BRIN indexes on timestamp columns
- The result is used internally by BRIN infrastructure to optimize block range selections