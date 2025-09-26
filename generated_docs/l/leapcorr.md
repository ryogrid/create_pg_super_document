# leapcorr

## Location
src/timezone/localtime.c: 1574 - 1609

## Overview
Calculates the leap second correction for a given timestamp based on timezone state information.

## Definition


## Detailed Description
The `leapcorr` function determines the cumulative leap second correction that should be applied to a given timestamp. It searches through the leap second information stored in the timezone state structure to find the appropriate correction value for the specified time.

The function iterates backwards through the leap second transition list, looking for the most recent leap second transition that occurred before or at the given timestamp. When found, it returns the cumulative leap second correction associated with that transition. If no applicable leap second transition is found (i.e., the timestamp is before any recorded leap seconds), it returns 0.

This function is essential for accurate time calculations in timezones that account for leap seconds, ensuring that time representations remain consistent with official time standards.

## Parameters / Member Variables
- `sp`: Pointer to the timezone state structure containing leap second information
- `t`: The timestamp for which to calculate the leap second correction

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_t (PostgreSQL time type)
  - lsinfo (leap second info structure)
- Called from (representative examples):
  - tzloadbody (during timezone data loading and validation)

## Notes and Other Information
- Returns the cumulative leap second correction as a 64-bit signed integer
- Returns 0 if no leap second correction applies to the given timestamp
- Searches leap second transitions in reverse chronological order for efficiency
- The function assumes leap second information is stored in chronological order
- Used internally during timezone file loading and processing
- The function is static and used within the timezone subsystem