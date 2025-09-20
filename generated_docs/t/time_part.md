# time_part

## Location
[src/backend/utils/adt/date.c:2243-2248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2243-L2248)

## Overview
The  function is a PostgreSQL wrapper that extracts specified time components from a TimeADT value, returning results as floating-point numbers.

## Definition

```c
Datum
time_part(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the public interface for PostgreSQL's  function when applied to time data types. It is a simple wrapper around , specifically configured to return floating-point results (float8) rather than numeric values. The function delegates all the actual processing work to  with the  parameter set to false.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - Text string specifying the time component to extract (e.g., 'hour', 'minute', 'second')
  - Argument 1:  - The time value to extract the component from

## Dependencies
- Functions called/Symbols referenced:
  - [time_part_common](time_part_common.md) (with retnumeric=false)
- Called from (representative examples):
  - No direct references found (likely called via SQL date_part() function system)

## Notes and Other Information
- This is a thin wrapper function that provides the float8 variant of time component extraction
- The companion function would be  which returns numeric values instead
- All actual functionality is implemented in 
- This function is typically invoked through PostgreSQL's SQL function system, particularly the  function
- Returns floating-point values, which may have precision limitations for very large or very precise time values
- Located in 