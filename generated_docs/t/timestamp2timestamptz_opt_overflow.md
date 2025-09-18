# timestamp2timestamptz_opt_overflow

## Location
src/backend/utils/adt/timestamp.c: 6304 - 6355

## Overview
Converts a timestamp to timestamptz with optional overflow handling, providing controlled behavior when the result would be out of valid range.

## Definition


## Detailed Description
This function converts a timestamp (without timezone) to a timestamptz (with timezone) while providing optional overflow handling. The conversion process interprets the input timestamp as being in the current session timezone and converts it to UTC. The function offers two modes of operation based on the overflow parameter:

1. If overflow is NULL: Throws an error when the result is out of valid timestamptz range
2. If overflow is not NULL: Sets the overflow indicator and returns appropriate infinity values instead of erroring

The conversion process involves decomposing the timestamp into components, determining the timezone offset for that time, and applying the offset to convert to UTC. If the resulting value exceeds the valid timestamp range, the function handles it according to the overflow parameter.

## Parameters / Member Variables
-  (Timestamp): The input timestamp value to be converted
-  (int*): Optional pointer to overflow indicator. If NULL, errors on overflow. If not NULL, receives -1 for negative overflow, +1 for positive overflow, or 0 for successful conversion.

## Dependencies
- Functions called/Symbols referenced:
  - TIMESTAMP_NOT_FINITE
  - timestamp2tm
  - DetermineTimeZoneOffset
  - dt2local
  - IS_VALID_TIMESTAMP
  - TIMESTAMP_NOBEGIN
  - TIMESTAMP_NOEND
  - session_timezone
  - pg_tm
  - fsec_t
  - MIN_TIMESTAMP
- Called from (representative examples):
  - timestamp_cmp_timestamptz_internal (in src/backend/utils/adt/timestamp.c:2330)
  - timestamp2timestamptz (in src/backend/utils/adt/timestamp.c:6358)
  - timestamptz_cmp_internal (in src/include/utils/timestamp.h:133)

## Notes and Other Information
- This is the core implementation function for timestamp to timestamptz conversion with overflow control
- Used by comparison functions that need to handle edge cases gracefully without throwing errors
- The overflow handling mechanism allows for robust comparison operations even with out-of-range values
- Infinite timestamps are passed through unchanged
- The function performs timezone offset calculation using the session's current timezone setting
- Critical for implementing timestamp comparison operations that must handle boundary conditions
- Located in src/backend/utils/adt/timestamp.c:6304-6355
- Error handling includes specific error code ERRCODE_DATETIME_VALUE_OUT_OF_RANGE when overflow parameter is NULL