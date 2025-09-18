# extract_time

## Location
[src/backend/utils/adt/date.c:2249-2262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2249-L2262)

## Overview
The extract_time function is a PostgreSQL SQL function wrapper that extracts date/time components from TimeADT values using the EXTRACT operator.

## Definition
```c
Datum extract_time(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL SQL-callable wrapper for extracting time components (hour, minute, second, etc.) from TimeADT values. It delegates the actual extraction logic to the time_part_common function with the is_time parameter set to true, indicating that it operates on time data types rather than timestamps.

The function follows PostgreSQL's version-1 calling convention, accepting arguments through the FunctionCallInfo structure and returning a Datum value.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the input arguments and context for the extraction operation

## Dependencies
- Functions called/Symbols referenced:
  - [time_part_common](../t/time_part_common.md)
- Called from (representative examples):
  - No direct callers found (typically invoked through PostgreSQL's SQL EXTRACT operator)

## Notes and Other Information
- This function is part of PostgreSQL's date/time type system in src/backend/utils/adt/date.c
- It specifically handles TIME type values as opposed to timestamp or other temporal types
- The function relies on time_part_common to perform the actual component extraction logic
- Typically invoked indirectly through SQL EXTRACT expressions rather than direct function calls