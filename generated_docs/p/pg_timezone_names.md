# pg_timezone_names

## Location
src/backend/utils/adt/datetime.c: 5122 - 5182

## Overview
A set-returning function that reads all available full time zones and returns a set of (name, abbrev, utc_offset, is_dst) for each timezone.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that enumerates all available timezone definitions and returns detailed information about each one. It's implemented as a set-returning function (SRF) that materializes its results in a tuplestore.

The function iterates through all available timezones using the timezone enumeration API, converts the current transaction start timestamp to local time in each timezone, and extracts timezone information including the timezone name, abbreviation, UTC offset, and daylight saving time status.

The function includes special handling for problematic timezone abbreviations, particularly rejecting ridiculously long abbreviations (over 31 characters) that were historically produced by IANA's "Factory" timezone or modified by some packagers.

## Parameters / Member Variables
This function takes no explicit parameters but uses the standard PostgreSQL function calling convention:
- Uses  macro for function arguments
- Returns a  (0 for SRF completion)
- Accesses result information via 

## Dependencies
- Functions called/Symbols referenced:
  -  - [Initialize](../I/Initialize.md) materialized set-returning function
  -  - Start timezone enumeration
  -  - Get next timezone in enumeration
  -  - End timezone enumeration
  -  - Get current transaction start time
  -  - Convert timestamp to broken-down time structure
  -  - Get canonical timezone name
  -  - Convert interval structure to Interval datum
  -  - Store tuple values in result set
  -  - Memory initialization utility
  - Various data conversion functions (, , )

- Called from:
  - This function is exposed as a SQL function and called directly from SQL queries, not typically called from other C functions

## Notes and Other Information
- Location: 
- This function is typically exposed to SQL as  system function
- Returns 4 columns: timezone name (text), abbreviation (text), UTC offset (interval), and DST flag (boolean)
- Filters out timezone abbreviations longer than 31 characters to prevent display issues
- Uses the current transaction start timestamp as the reference point for timezone conversions
- The UTC offset is returned as a negative interval (positive values indicate time zones west of UTC)
- Handles conversion failures gracefully by skipping problematic timezones
- Part of PostgreSQL's timezone support infrastructure