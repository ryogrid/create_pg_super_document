# cmpDateToTimestamp

## Location
[src/backend/utils/adt/jsonpath_exec.c:3690-3698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3690-L3698)

## Overview
A static helper function that compares a date value to a timestamp value without timezone considerations during JSON path execution.

## Definition
```c
static int cmpDateToTimestamp(DateADT date1, Timestamp ts2, bool useTz)
```

## Detailed Description
This function performs a comparison between a date and timestamp value by delegating to PostgreSQL's internal date_cmp_timestamp_internal function. Notably, this comparison does not involve any timezone considerations, making it suitable for timezone-agnostic date/timestamp comparisons. The function serves as a wrapper that standardizes the comparison interface for use within the JSON path execution engine while maintaining consistency with other temporal comparison functions.

## Parameters / Member Variables
- `date1`: The date value to compare (DateADT type)
- `ts2`: The timestamp value to compare against (Timestamp type)  
- `useTz`: Boolean flag for timezone usage (not used in this function but maintained for interface consistency)

## Dependencies
- Functions called/Symbols referenced:
  - [date_cmp_timestamp_internal](../d/date_cmp_timestamp_internal.md) (core comparison function)
  - DateADT (date data type)
  - Timestamp (timestamp data type)
- Called from (representative examples):
  - [compareDatetime](compareDatetime.md) (multiple locations for datetime comparisons)

## Notes and Other Information
- Located in src/backend/utils/adt/jsonpath_exec.c:3690-3698
- Part of PostgreSQL's JSON path execution engine for temporal data comparisons
- Explicitly documented as not involving timezone considerations
- Returns an integer comparison result (negative, zero, or positive)
- The useTz parameter is present for interface consistency but not utilized in the comparison logic
- Uses PostgreSQL's internal date comparison infrastructure for reliability and consistency

## Simplified Source

```c
static int
cmpDateToTimestamp(DateADT date1, Timestamp ts2, bool useTz)
{
    // Direct comparison of date to timestamp without timezone considerations
    return date_cmp_timestamp_internal(date1, ts2);
}
```