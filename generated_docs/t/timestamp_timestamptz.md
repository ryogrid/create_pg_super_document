# timestamp_timestamptz

## Location
src/backend/utils/adt/timestamp.c: 6286 - 6303

## Overview
Converts a local timestamp to a timestamptz (timestamp with timezone) by interpreting the timestamp as being in the current session timezone.

## Definition


## Detailed Description
This function converts a timestamp (without timezone) to a timestamptz (with timezone) by treating the input timestamp as if it were in the current session timezone and converting it to GMT/UTC. The function is a simple wrapper around the timestamp2timestamptz function, which performs the actual conversion logic. The conversion takes into account the current session's timezone setting to determine how to interpret the input timestamp value.

## Parameters / Member Variables
-  (Timestamp): The input timestamp value without timezone information that needs to be converted to timestamptz.

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP
  - timestamp2timestamptz
  - PG_RETURN_TIMESTAMPTZ
- Called from (representative examples):
  - executeDateTimeMethod (in src/backend/utils/adt/jsonpath_exec.c:2736)
  - timestamp_at_local (in src/backend/utils/adt/timestamp.c:6694)

## Notes and Other Information
- This function is the SQL-callable wrapper for timestamp to timestamptz conversion
- The actual conversion logic is delegated to timestamp2timestamptz function
- The conversion assumes the input timestamp is in the current session timezone
- Used in JSON path operations and timestamp-at-timezone calculations
- This is one of the core type conversion functions in PostgreSQL's timestamp handling system
- Located in src/backend/utils/adt/timestamp.c:6286-6303
- The function follows PostgreSQL's convention for SQL-callable functions using the PG_FUNCTION_ARGS interface