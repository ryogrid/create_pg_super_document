# timestamp_cmp_timestamptz

## Location
[src/backend/utils/adt/timestamp.c:2400-2408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2400-L2408)

## Overview
A PostgreSQL function that compares a timestamp (without timezone) value with a timestamptz (with timezone) value, returning an integer indicating their relative ordering.

## Definition

```c
Datum
timestamp_cmp_timestamptz(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL-callable wrapper for comparing timestamp and timestamptz values. It extracts the timestamp and timestamptz arguments from the PostgreSQL function call context, then delegates the actual comparison logic to the internal function . The function returns a standard comparison result: negative for less-than, zero for equal, and positive for greater-than relationships.

The comparison handles timezone conversion by converting the plain timestamp to timestamptz using the session's timezone setting, then performing the comparison. This allows cross-type comparisons between timestamp and timestamptz data types in PostgreSQL.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - Argument 0:  - The timestamp value (without timezone) to compare
  - Argument 1:  - The timestamptz value (with timezone) to compare against

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts timestamp argument from function call context
  -  - Extracts timestamptz argument from function call context  
  -  - Performs the actual comparison logic
  -  - Returns the comparison result as a 32-bit integer
  -  - Data type for timestamp without timezone
- Called from (representative examples):
  - PostgreSQL query execution engine (not directly referenced in codebase)

## Notes and Other Information
- This function is part of PostgreSQL's cross-type comparison infrastructure
- The actual comparison logic handles timezone conversion and special timestamp values (infinity, -infinity)
- Used internally by PostgreSQL's operator system for timestamp/timestamptz comparisons
- Located at src/backend/utils/adt/timestamp.c:2400-2408

## Simplified Source

```c
Datum timestamp_cmp_timestamptz(PG_FUNCTION_ARGS) {
    // Extract timestamp (without timezone) and timestamptz (with timezone)
    Timestamp timestamp_val = PG_GETARG_TIMESTAMP(0);
    TimestampTz timestamptz_val = PG_GETARG_TIMESTAMPTZ(1);

    // Return comparison result: negative (<), zero (=), or positive (>)
    PG_RETURN_INT32(timestamp_cmp_timestamptz_internal(timestamp_val, timestamptz_val));
}
```