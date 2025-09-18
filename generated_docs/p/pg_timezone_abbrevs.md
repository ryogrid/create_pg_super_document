# pg_timezone_abbrevs

## Location
[src/backend/utils/adt/datetime.c:5001-5121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L5001-L5121)

## Overview
pg_timezone_abbrevs is a set-returning SQL function that provides access to all available timezone abbreviations in the current PostgreSQL configuration, returning their names, UTC offsets, and daylight saving time status.

## Definition


## Detailed Description
This SQL-accessible function exposes PostgreSQL's internal timezone abbreviation table as a queryable result set. It returns three columns for each timezone abbreviation:
- **abbrev**: The timezone abbreviation name (uppercased)
- **utc_offset**: Time offset from UTC as an interval 
- **is_dst**: Boolean indicating if this represents daylight saving time

The function handles three types of timezone entries:
1. **TZ (Static timezone)**: Fixed UTC offset, not daylight saving time
2. **DTZ (Daylight timezone)**: Fixed UTC offset, represents daylight saving time
3. **DYNTZ (Dynamic timezone)**: Variable offset determined by timezone rules at current timestamp

For dynamic timezones, the function performs real-time resolution using the current transaction timestamp to determine the appropriate offset and DST status. This ensures the returned information reflects current timezone rules rather than historical or future states.

The function uses PostgreSQL's Set-Returning Function (SRF) framework to iterate through all entries in the active timezone abbreviation table, maintaining state between calls through the function context.

## Parameters / Member Variables
- No input parameters (uses PG_FUNCTION_ARGS macro for SRF framework)
- Returns: Set of records with (text abbrev, interval utc_offset, boolean is_dst)

## Dependencies
- Functions called/Symbols referenced:
  - SRF framework macros (SRF_IS_FIRSTCALL, SRF_FIRSTCALL_INIT, etc.)
  - [FetchDynamicTimeZone](../F/FetchDynamicTimeZone.md) (resolve dynamic timezone abbreviations)
  - [GetCurrentTransactionStartTimestamp](../G/GetCurrentTransactionStartTimestamp.md) (get current time for dynamic resolution)
  - [DetermineTimeZoneAbbrevOffsetTS](../D/DetermineTimeZoneAbbrevOffsetTS.md) (calculate timezone offset at specific time)
  - DateTimeParseError (report timezone resolution errors)
  - Memory management (palloc, MemoryContextSwitchTo)
  - String/data conversion utilities (strlcpy, pg_toupper, itmin2interval)
- Data structures referenced:
  - TimeZoneAbbrevTable (via global zoneabbrevtbl)
  - [FuncCallContext](../F/FuncCallContext.md) (SRF state management)
  - datetkn (timezone token entries)
  - pg_itm_in, Interval (time interval representation)
- Called from:
  - SQL queries via function call mechanism

## Notes and Other Information
- This function provides the underlying data for the pg_timezone_abbrevs view
- Abbreviation names are converted to uppercase to match PostgreSQL's internal representation
- Dynamic timezone resolution uses the current transaction start time to ensure consistency within a transaction
- The function will raise an error if dynamic timezone resolution fails (e.g., invalid timezone name in configuration)
- Results reflect the currently active timezone_abbreviations configuration setting
- Memory allocated during function execution is automatically cleaned up by the SRF framework
- Returns empty result set if no timezone abbreviation table is loaded