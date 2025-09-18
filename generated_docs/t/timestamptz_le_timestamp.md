# timestamptz_le_timestamp

## Location
src/backend/utils/adt/timestamp.c: 2445 - 2453

## Overview
Compares a timestamp with timezone (timestamptz) value with a plain timestamp value and returns true if the timestamptz value is less than or equal to the timestamp value.

## Definition
```c
Datum timestamptz_le_timestamp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the "<=" comparison operator between a timestamptz (timestamp with timezone) and a plain timestamp. It extracts the two arguments from the PostgreSQL function call framework, converts them to appropriate internal representations, and uses the internal comparison function `timestamp_cmp_timestamptz_internal` to perform the actual comparison. The function returns true if the timestamptz value is less than or equal to the timestamp value when compared in a timezone-aware manner.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: TimestampTz value (timestamp with timezone)
  - Argument 1: Timestamp value (plain timestamp without timezone)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMPTZ (macro to extract timestamptz argument)
  - PG_GETARG_TIMESTAMP (macro to extract timestamp argument)
  - timestamp_cmp_timestamptz_internal (internal comparison function)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from:
  - No direct references found (likely called through PostgreSQL's operator framework)

## Notes and Other Information
- This function is part of PostgreSQL's timestamp comparison operator implementation
- The comparison is performed by the internal function which handles timezone considerations
- Returns true when the result of the internal comparison is >= 0
- Located in src/backend/utils/adt/timestamp.c:2445-2453