# interval_justify_days

## Location
[src/backend/utils/adt/timestamp.c:3002-3048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3002-L3048)

## Overview
Adjusts an interval so that the 'day' component contains less than 30 days, transferring the excess to the 'month' component.

## Definition

```c
Datum
interval_justify_days(PG_FUNCTION_ARGS)
```
## Detailed Description
This function normalizes PostgreSQL interval values by converting days exceeding 30 into months. It takes an Interval structure and redistributes the day component when it exceeds DAYS_PER_MONTH (30 days). The function handles both positive and negative intervals correctly and ensures the result maintains proper sign consistency between month and day components.

The function performs the following operations:
1. Calculates how many whole months can be extracted from the day component
2. Adjusts the month component by adding the calculated whole months
3. Reduces the day component by removing the converted days
4. Handles sign normalization to ensure month and day components have consistent signs

## Parameters / Member Variables
- Input: An Interval pointer obtained via  containing the original interval
- Returns: A Datum containing a normalized Interval structure

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P
  - [palloc](../p/palloc.md)
  - INTERVAL_NOT_FINITE
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
  - ereport
  - PG_RETURN_INTERVAL_P
- Constants used:
  - DAYS_PER_MONTH (30)
- Called from:
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- The function handles infinite intervals by returning them unchanged
- Overflow detection is implemented using pg_add_s32_overflow to prevent integer overflow
- Sign normalization ensures that month and day components don't have opposite signs
- This function is part of PostgreSQL's interval arithmetic system and is exposed as a SQL function
- The function follows PostgreSQL's standard pattern for SQL-callable functions using PG_FUNCTION_ARGS