# castTimeToTimeTz

## Location
[src/backend/utils/adt/jsonpath_exec.c:3678-3689](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3678-L3689)

## Overview
A static helper function that converts a time datum to a timetz (time with timezone) datum during JSON path execution.

## Definition
```c
static Datum castTimeToTimeTz(Datum time, bool useTz)
```

## Detailed Description
This function performs a type conversion from a PostgreSQL time data type to timetz (time with timezone). It first validates that timezone usage is properly enabled for this cast operation through checkTimezoneIsUsedForCast, then delegates the actual conversion to the built-in time_timetz function. This ensures that timezone-sensitive conversions are properly handled and prevents incorrect temporal operations that might occur without timezone context.

## Parameters / Member Variables
- `time`: The input time datum to be converted
- `useTz`: Boolean flag indicating whether timezone usage is enabled for the cast operation

## Dependencies
- Functions called/Symbols referenced:
  - [checkTimezoneIsUsedForCast](checkTimezoneIsUsedForCast.md) (validation)
  - DirectFunctionCall1 (function call wrapper)
  - [time_timetz](../t/time_timetz.md) (core conversion function)
- Called from (representative examples):
  - [compareDatetime](compareDatetime.md) (multiple locations for datetime comparisons)

## Notes and Other Information
- Located in src/backend/utils/adt/jsonpath_exec.c:3678-3689
- Part of PostgreSQL's JSON path execution engine for temporal data handling
- Essential for datetime comparison operations that require timezone awareness
- The function ensures type safety by validating timezone requirements before conversion
- Returns a Datum containing the converted timetz value
- Uses PostgreSQL's DirectFunctionCall1 infrastructure for efficient function dispatch