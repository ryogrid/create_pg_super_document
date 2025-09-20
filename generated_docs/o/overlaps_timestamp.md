# overlaps_timestamp

## Location
[src/backend/utils/adt/timestamp.c:2631-2646](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2631-L2646)

## Overview
This function implements the SQL OVERLAPS operator for timestamp data types, determining whether two timestamp intervals overlap according to SQL specification rules.

## Definition

```c
Datum
overlaps_timestamp(PG_FUNCTION_ARGS)
```
## Detailed Description
The overlaps_timestamp function implements the SQL OVERLAPS operator for timestamp values without timezone information. It takes four timestamp arguments representing two intervals: (ts1, te1) and (ts2, te2), and determines whether these intervals overlap. The implementation follows the SQL specification exactly, which requires handling null values in a specific way to deliver non-null results in certain cases where some inputs are null. The algorithm handles three main cases based on the relationship between the start points of the intervals: ts1 > ts2, ts1 < ts2, and ts1 = ts2.

## Parameters / Member Variables
The function takes four arguments via PG_FUNCTION_ARGS:
- : First timestamp of the first interval (argument 0)
- : Second timestamp of the first interval (argument 1) 
- : First timestamp of the second interval (argument 2)
- : Second timestamp of the second interval (argument 3)

Internal variables:
- , , , : Boolean flags tracking null status of each argument
- , : Macros for timestamp comparison operations

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATUM
  - PG_ARGISNULL
  - DirectFunctionCall2
  - [timestamp_gt](../t/timestamp_gt.md)
  - [timestamp_lt](../t/timestamp_lt.md)
  - [DatumGetBool](../D/DatumGetBool.md)
  - PG_RETURN_NULL
  - PG_RETURN_BOOL
- Called from (representative examples):
  - SQL OVERLAPS operator expressions
  - System catalog function definitions

## Notes and Other Information
- The function carefully handles null values according to SQL specification requirements
- Arguments are kept as generic Datums to avoid unnecessary conversions and potential null pointer dereferences
- The algorithm normalizes intervals so that the first timestamp is always the lesser endpoint when both endpoints are non-null
- Uses helper macros TIMESTAMP_GT and TIMESTAMP_LT for cleaner comparison code
- Returns null when both endpoints of either interval are null, or when the overlap determination requires comparing null values
- The implementation is more complex than might be expected due to the SQL specification's requirements for null handling in temporal overlaps