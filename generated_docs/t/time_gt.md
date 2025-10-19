# time_gt

## Location
[src/backend/utils/adt/date.c:1716-1724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1716-L1724)

## Overview
Compares two time values to determine if the first time is greater than the second time.

## Definition
```c
Datum time_gt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `time_gt` function is a PostgreSQL built-in function that implements the greater-than comparison operator ('>') for the TIME data type. It takes two TimeADT values as arguments and returns a boolean indicating whether the first time value is chronologically later than the second time value. This function is part of PostgreSQL's type system for handling temporal comparisons and is typically invoked through SQL queries using the '>' operator on time columns.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `time1` (TimeADT): The first time value to compare (extracted from argument 0)
  - `time2` (TimeADT): The second time value to compare (extracted from argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMEADT: Macro to extract TimeADT values from function arguments
  - PG_RETURN_BOOL: Macro to return boolean result
  - TimeADT: PostgreSQL's internal representation of time values
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/date.c:1716-1724
- Part of PostgreSQL's comprehensive set of time comparison functions
- Uses simple numeric comparison since TimeADT is internally represented as microseconds since midnight
- Returns true if time1 > time2, false otherwise

## Simplified Source

```c
bool time_gt(TimeADT time1, TimeADT time2) {
    // Return true if first time is later than second
    return time1 > time2;
}
```