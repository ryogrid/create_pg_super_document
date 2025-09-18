# timestamptz_zone

## Location
src/backend/utils/adt/timestamp.c: 6402 - 6465

## Overview
This function evaluates a timestamp with time zone at a specified time zone and returns the corresponding timestamp without time zone, effectively converting a timestamptz to local time in the target timezone.

## Definition


## Detailed Description
The  function converts a timestamp with time zone to a plain timestamp by interpreting the timestamptz value in a specified target timezone rather than the session's current timezone. This function accepts two arguments: a timezone specification (as text) and a timestamptz value.

The function handles three different types of timezone specifications:

1. **Fixed-offset abbreviations** (like '+05:00', 'PST-8'): The function applies the fixed offset directly using .

2. **Dynamic-offset abbreviations** (like 'PST', 'EDT'): These abbreviations can have different offsets depending on daylight saving time rules. The function uses  to resolve the actual offset at the given timestamp.

3. **Full zone names** (like 'America/New_York', 'Europe/London'): For complete timezone names, the function performs a full timezone conversion by decomposing the timestamptz with the target timezone using , then reconstructing it as a plain timestamp.

The function includes comprehensive error handling for out-of-range values and invalid timestamps, and properly handles non-finite timestamp values by passing them through unchanged.

## Parameters / Member Variables
- Argument 0:  (text) - The target timezone specification as a text string
- Argument 1:  (TimestampTz) - The input timestamp with timezone value to convert

## Dependencies
- Functions called/Symbols referenced:
  -  - retrieves the timezone text argument
  -  - retrieves the timestamptz argument
  -  - checks for infinite timestamp values
  -  - converts text to C string
  -  - parses and categorizes timezone names
  -  - applies timezone offset to convert timestamptz to local time
  -  - resolves dynamic timezone abbreviations
  -  - decomposes timestamp with timezone consideration
  -  - reconstructs timestamp from components
  -  - validates the resulting timestamp
  -  - returns the converted timestamp result
- Called from:
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This function implements the PostgreSQL SQL function for timezone conversion (likely accessible via AT TIME ZONE syntax)
- The function supports all PostgreSQL timezone specification formats including abbreviations, offsets, and full zone names
- Located in  at lines 6402-6465
- Handles daylight saving time transitions correctly for dynamic timezone abbreviations
- Uses a maximum timezone name length of  characters
- The function follows PostgreSQL's V1 calling convention for SQL functions
- Comprehensive error reporting ensures that invalid timezone names or out-of-range timestamps are properly handled
- The result is always a plain timestamp (without timezone information) representing the local time in the specified timezone