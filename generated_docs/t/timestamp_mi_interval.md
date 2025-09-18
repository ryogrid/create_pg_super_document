# timestamp_mi_interval

## Location
src/backend/utils/adt/timestamp.c: 3166 - 3191

## Overview
Subtracts an interval from a timestamp by negating the interval and then using the timestamp addition function.

## Definition


## Detailed Description
This function implements timestamp-interval subtraction by leveraging the existing timestamp_pl_interval function. It follows a simple but effective approach:

1. Takes the input timestamp and interval
2. Negates the interval using interval_um_internal (unary minus for intervals)
3. Delegates to timestamp_pl_interval with the negated interval

This design avoids code duplication by reusing all the complex calendar arithmetic, infinity handling, and overflow detection logic already implemented in timestamp_pl_interval.

## Parameters / Member Variables
- Input parameter 0: Timestamp value via 
- Input parameter 1: Interval pointer via 
- Returns: A Datum containing the resulting timestamp after subtraction

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP, PG_GETARG_INTERVAL_P (parameter extraction)
  - [interval_um_internal](../i/interval_um_internal.md) (interval negation)
  - [timestamp_pl_interval](timestamp_pl_interval.md) (delegated addition with negated interval)
  - DirectFunctionCall2 (direct function call mechanism)
  - TimestampGetDatum, PointerGetDatum (datum conversion)
- Called from:
  - [date_mi_interval](../d/date_mi_interval.md) (src/backend/utils/adt/date.c:1274)
  - [in_range_timestamp_interval](../i/in_range_timestamp_interval.md) (src/backend/utils/adt/timestamp.c:3861)

## Notes and Other Information
- Demonstrates PostgreSQL's efficient code reuse pattern - subtraction is implemented as addition with negation
- All the complex handling (infinities, calendar arithmetic, overflow detection) is inherited from timestamp_pl_interval
- The function creates a local copy of the interval (tspan) for negation rather than modifying the original
- Uses DirectFunctionCall2 to invoke timestamp_pl_interval directly, avoiding SQL function call overhead
- Part of PostgreSQL's temporal arithmetic system, commonly used in date/time calculations and window functions