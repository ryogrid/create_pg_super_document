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

## Simplified Source

```c
int
DecodeDateTime(char **field, int *ftype, int nf,
               int *dtype, struct pg_tm *tm, fsec_t *fsec, int *tzp,
               DateTimeErrorExtra *extra)
{
    int fmask = 0;  // Track which fields we've seen
    int mer = HR24; // AM/PM indicator
    bool haveTextMonth = false;
    pg_tz *namedTz = NULL;
    pg_tz *abbrevTz = NULL;

    // Initialize output structure
    *dtype = DTK_DATE;
    tm->tm_hour = tm->tm_min = tm->tm_sec = 0;
    *fsec = 0;
    tm->tm_isdst = -1;
    if (tzp != NULL) *tzp = 0;

    // Process each field
    for (int i = 0; i < nf; i++) {
        int tmask;

        switch (ftype[i]) {
            case DTK_DATE:
                // Handle date field - may be simple date or timezone
                if (fmask & (DTK_M(MONTH) | DTK_M(DAY))) {
                    // Already have date parts, this might be timezone
                    if (tzp == NULL) return DTERR_BAD_FORMAT;

                    // Try to decode as timezone
                    if (isdigit(*field[i])) {
                        DecodeTimezone(field[i], tzp);
                        tmask = DTK_M(TZ);
                    } else {
                        namedTz = pg_tzset(field[i]);
                        if (!namedTz) return DTERR_BAD_TIMEZONE;
                        tmask = DTK_M(TZ);
                    }
                } else {
                    // Decode as date
                    DecodeDate(field[i], fmask, &tmask, &is2digits, tm);
                }
                break;

            case DTK_TIME:
                // Decode time field
                DecodeTime(field[i], fmask, INTERVAL_FULL_RANGE, &tmask, tm, fsec);
                if (time_overflows(tm->tm_hour, tm->tm_min, tm->tm_sec, *fsec))
                    return DTERR_FIELD_OVERFLOW;
                break;

            case DTK_TZ:
                // Timezone offset
                if (tzp == NULL) return DTERR_BAD_FORMAT;
                DecodeTimezone(field[i], tzp);
                tmask = DTK_M(TZ);
                break;

            case DTK_NUMBER:
                // Handle numeric fields - could be date, time, or Julian day
                DecodeNumber(strlen(field[i]), field[i], haveTextMonth, fmask,
                           &tmask, tm, fsec, &is2digits);
                break;

            case DTK_STRING:
            case DTK_SPECIAL:
                // Handle special strings (month names, 'now', 'today', etc.)
                DecodeSpecial(i, field[i], &val);

                switch (type) {
                    case MONTH:
                        haveTextMonth = true;
                        tm->tm_mon = val;
                        tmask = DTK_M(MONTH);
                        break;

                    case RESERV:
                        // Handle 'now', 'today', 'yesterday', etc.
                        switch (val) {
                            case DTK_NOW:
                                GetCurrentTimeUsec(tm, fsec, tzp);
                                tmask = DTK_DATE_M | DTK_TIME_M | DTK_M(TZ);
                                break;
                            case DTK_TODAY:
                                GetCurrentDateTime(&cur_tm);
                                tm->tm_year = cur_tm.tm_year;
                                tm->tm_mon = cur_tm.tm_mon;
                                tm->tm_mday = cur_tm.tm_mday;
                                tmask = DTK_DATE_M;
                                break;
                        }
                        break;

                    case AMPM:
                        mer = val;
                        break;
                }
                break;
        }

        // Check for duplicate field types
        if (tmask & fmask) return DTERR_BAD_FORMAT;
        fmask |= tmask;
    }

    // Post-processing and validation
    if (*dtype == DTK_DATE) {
        // Validate date components
        ValidateDate(fmask, isjulian, is2digits, bc, tm);

        // Handle AM/PM conversion
        if (mer == AM && tm->tm_hour == 12) tm->tm_hour = 0;
        else if (mer == PM && tm->tm_hour != 12) tm->tm_hour += 12;

        // Resolve timezone if needed
        if (namedTz != NULL) {
            *tzp = DetermineTimeZoneOffset(tm, namedTz);
        } else if (abbrevTz != NULL) {
            *tzp = DetermineTimeZoneAbbrevOffset(tm, abbrev, abbrevTz);
        } else if (tzp != NULL && !(fmask & DTK_M(TZ))) {
            *tzp = DetermineTimeZoneOffset(tm, session_timezone);
        }

        // Check for incomplete input
        if ((fmask & DTK_DATE_M) != DTK_DATE_M) {
            if ((fmask & DTK_TIME_M) == DTK_TIME_M) return 1;
            return DTERR_BAD_FORMAT;
        }
    }

    return 0;
}
```