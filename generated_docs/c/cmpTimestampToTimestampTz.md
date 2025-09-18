# cmpTimestampToTimestampTz

## Location
[src/backend/utils/adt/jsonpath_exec.c:3710-3722](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3710-L3722)

## Overview
A static helper function that compares a timestamp value to a timestamptz (timestamp with timezone) value with timezone validation during JSON path execution.

## Definition
```c
static int cmpTimestampToTimestampTz(Timestamp ts1, TimestampTz tstz2, bool useTz)
```

## Detailed Description
This function performs a comparison between a regular timestamp and a timezone-aware timestamptz value. Like other timezone-sensitive comparison functions, it first validates that timezone usage is properly enabled through checkTimezoneIsUsedForCast. This validation is crucial because comparing a timezone-naive timestamp with a timezone-aware timestamp can lead to incorrect results without proper timezone context. After validation, it delegates to PostgreSQL's internal timestamp_cmp_timestamptz_internal function for the actual comparison.

## Parameters / Member Variables
- `ts1`: The regular timestamp value to compare (Timestamp type)
- `tstz2`: The timezone-aware timestamp value to compare against (TimestampTz type)
- `useTz`: Boolean flag indicating whether timezone usage is enabled for the comparison

## Dependencies
- Functions called/Symbols referenced:
  - [checkTimezoneIsUsedForCast](checkTimezoneIsUsedForCast.md) (timezone validation)
  - [timestamp_cmp_timestamptz_internal](../t/timestamp_cmp_timestamptz_internal.md) (core comparison function)
  - Timestamp (timestamp data type)
  - TimestampTz (timezone-aware timestamp data type)
- Called from (representative examples):
  - [compareDatetime](compareDatetime.md) (multiple locations for datetime comparisons)

## Notes and Other Information
- Located in src/backend/utils/adt/jsonpath_exec.c:3710-3722
- Part of PostgreSQL's JSON path execution engine for mixed timezone temporal comparisons
- Essential for handling comparisons between timezone-naive and timezone-aware temporal values
- Returns an integer comparison result (negative, zero, or positive)
- Requires explicit timezone context to ensure meaningful comparison results
- Will raise an error if timezone usage is not enabled, preventing potentially misleading timestamp comparisons