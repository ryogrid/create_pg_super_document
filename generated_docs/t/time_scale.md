# time_scale

## Location
src/backend/utils/adt/date.c: 1625 - 1644

## Overview
A PostgreSQL function that adjusts TIME data type values for specified scale factors, used by the type system to handle column precision constraints.

## Definition
```c
Datum time_scale(PG_FUNCTION_ARGS)
```

## Detailed Description
The time_scale function is responsible for adjusting TIME data type values according to specified scale factors (precision). This function is used internally by PostgreSQL's type system to "stuff columns" - meaning it applies precision constraints to TIME values when they are stored or converted. The function takes a TIME value and a typmod (type modifier) parameter that specifies the desired precision, then calls AdjustTimeForTypmod to perform the actual adjustment. This ensures that TIME values conform to the precision requirements defined in table schemas or explicit type casts.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro
  - `time`: TimeADT value representing the input time to be scaled
  - `typmod`: int32 type modifier that specifies the desired precision/scale

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMEADT (macro for extracting TimeADT arguments)
  - TimeADT (data type for time values)
  - [AdjustTimeForTypmod](../A/AdjustTimeForTypmod.md) (function that performs the actual time adjustment)
  - PG_RETURN_TIMEADT (macro for returning TimeADT values)
- Called from (representative examples):
  - No direct references found in the codebase (likely used through function registration)

## Notes and Other Information
- This function works in conjunction with time_support() to provide complete support for TIME type precision handling
- The function creates a copy of the input time value before adjustment to avoid modifying the original
- Located in src/backend/utils/adt/date.c:1625-1644
- Part of PostgreSQL's type coercion system for maintaining data type constraints