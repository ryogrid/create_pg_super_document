# timestamptz_lt_timestamp

## Location
[src/backend/utils/adt/timestamp.c:2427-2435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2427-L2435)

## Overview
A PostgreSQL function that tests whether a timestamptz (with timezone) value is less than a timestamp (without timezone) value, returning a boolean result.

## Definition

```c
Datum
timestamptz_lt_timestamp(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL-callable wrapper for testing less-than relationships between timestamptz and timestamp values. It extracts both arguments from the PostgreSQL function call context and uses the internal comparison function  to determine the ordering. The function returns true if the timestamptz value is less than the timestamp value (i.e., when the comparison result is greater than 0, indicating the timestamp is greater than the timestamptz).

The less-than test handles timezone conversion by converting the plain timestamp to timestamptz using the session's timezone setting, then performing the comparison. This enables cross-type ordering operations between timestamptz and timestamp data types in PostgreSQL's type system.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - Argument 0:  - The timestamptz value (with timezone) to compare
  - Argument 1:  - The timestamp value (without timezone) to compare against

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts timestamptz argument from function call context
  -  - Extracts timestamp argument from function call context
  -  - Performs the actual comparison logic
  -  - Returns the comparison result as a boolean value
  -  - Data type for timestamp with timezone
  -  - Data type for timestamp without timezone
- Called from (representative examples):
  - PostgreSQL query execution engine (not directly referenced in codebase)

## Notes and Other Information
- This function implements the  operator for timestamptz < timestamp comparisons
- Part of PostgreSQL's cross-type comparison infrastructure for temporal data types
- The comparison logic checks if , which means the timestamp is greater than the timestamptz, hence timestamptz < timestamp
- The underlying comparison handles timezone conversion and special timestamp values
- Used internally by PostgreSQL's operator system and can be called from SQL queries
- Located at src/backend/utils/adt/timestamp.c:2427-2435