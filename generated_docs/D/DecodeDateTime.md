# DecodeDateTime

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:1780-2351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L1780-L2351)

## Overview
DecodeDateTime is the central function that interprets previously parsed date/time fields and converts them into structured date and time components for PostgreSQL's datetime types.

## Definition
```c
int DecodeDateTime(char **field, int *ftype, int nf,
                  int *dtype, struct pg_tm *tm, fsec_t *fsec, int *tzp,
                  DateTimeErrorExtra *extra)
```

## Detailed Description
DecodeDateTime is PostgreSQL's comprehensive date/time interpretation engine that processes tokenized fields from ParseDateTime and converts them into structured date and time information. It handles an extensive variety of input formats including ISO dates, Julian dates, compact time formats, relative dates (now, today, yesterday, tomorrow), timezone specifications, and special values (epoch, infinity). The function performs extensive validation, timezone resolution, AM/PM conversion, and daylight saving time handling. It supports both absolute and relative date specifications and can handle timezone-aware timestamps.

## Parameters / Member Variables
- `field[]`: Array of parsed field strings from ParseDateTime
- `ftype[]`: Array of field type indicators (DTK_DATE, DTK_TIME, DTK_TZ, etc.)
- `nf`: Number of fields in the field and ftype arrays
- `*dtype`: Output parameter indicating the result type (DTK_DATE for normal dates, or special values like DTK_EPOCH)
- `tm`: Output pg_tm structure containing decoded date and time components
- `*fsec`: Output parameter for fractional seconds (microseconds)
- `*tzp`: Output parameter for timezone offset in seconds (NULL if timezone not needed)
- `extra`: Structure for additional error information (e.g., unrecognized timezone names)

## Dependencies
- Functions called/Symbols referenced:
  - [DecodeTimezone](DecodeTimezone.md), DecodeTime, DecodeDate, DecodeNumber, DecodeNumberField
  - [DecodeSpecial](DecodeSpecial.md), DecodeTimezoneAbbrev
  - [ValidateDate](../V/ValidateDate.md), ParseFraction
  - [j2date](../j/j2date.md), date2j, dt2time
  - [GetCurrentTimeUsec](../G/GetCurrentTimeUsec.md), GetCurrentDateTime
  - [DetermineTimeZoneOffset](DetermineTimeZoneOffset.md), DetermineTimeZoneAbbrevOffset
  - [pg_tzset](../p/pg_tzset.md), time_overflows
  - Various DTK_* constants and DTERR_* error codes
- Called from (representative examples):
  - [date_in](../d/date_in.md), timestamp_in, timestamptz_in
  - [check_recovery_target_time](../c/check_recovery_target_time.md)
  - ECPG datetime parsing functions

## Notes and Other Information
- This is the main date/time interpretation function in PostgreSQL's datetime processing pipeline
- Handles complex logic for timezone resolution including named timezones and DST
- Supports special date values like 'now', 'today', 'yesterday', 'tomorrow', 'epoch', 'infinity'
- Performs comprehensive validation including range checking and format consistency
- Returns 0 for full date, 1 for time-only (treated as error by most callers), negative DTERR codes for errors
- Critical component used by all PostgreSQL date/time input functions
- Handles both backend and ECPG client library datetime processing needs