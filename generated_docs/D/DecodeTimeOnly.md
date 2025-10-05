# DecodeTimeOnly

## Location
[src/backend/utils/adt/datetime.c:1864-2397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L1864-L2397)

## Overview
Interprets parsed string fields as time-only values with optional timezone support, handling various time formats including ISO time, Julian dates, timezone abbreviations, and special keywords.

## Definition
```c
int DecodeTimeOnly(char **field, int *ftype, int nf, int *dtype, struct pg_tm *tm, fsec_t *fsec, int *tzp, DateTimeErrorExtra *extra)
```

## Detailed Description
This function processes an array of parsed time-related string fields and interprets them as time values, with optional timezone information. It's designed specifically for SQL TIME and TIME WITH TIME ZONE types. The function handles numerous time formats and representations:

- Standard time formats (HH:MM:SS, HH:MM)
- ISO time format following 't' prefix
- Julian date representations with optional fractional time
- Timezone abbreviations and full timezone names
- Special keywords like 'now', 'zulu'
- AM/PM modifiers
- Daylight saving time modifiers

The function performs extensive validation and field mask checking to ensure consistent time representation. It handles timezone resolution through multiple methods:
1. Named timezones (e.g., 'America/New_York')
2. Dynamic timezone abbreviations that require date context
3. Static timezone offsets
4. Session timezone as fallback

## Parameters / Member Variables
- `field`: Array of parsed string fields to interpret
- `ftype`: Array indicating the type of each field (DTK_DATE, DTK_TIME, DTK_NUMBER, etc.)
- `nf`: Number of fields in the arrays
- `dtype`: Output parameter set to DTK_TIME or DTK_DATE
- `tm`: Output pg_tm structure containing the decoded time components
- `fsec`: Output fractional seconds component
- `tzp`: Output timezone offset in seconds (NULL if timezone not supported)
- `extra`: Structure for additional error information

## Dependencies
- Functions called/Symbols referenced:
  - [DecodeDate](DecodeDate.md)
  - [DecodeTime](DecodeTime.md)
  - [DecodeTimezone](DecodeTimezone.md)
  - [DecodeNumberField](DecodeNumberField.md)
  - [DecodeNumber](DecodeNumber.md)
  - [DecodeSpecial](DecodeSpecial.md)
  - [DecodeTimezoneAbbrev](DecodeTimezoneAbbrev.md)
  - [DetermineTimeZoneOffset](DetermineTimeZoneOffset.md)
  - [DetermineTimeZoneAbbrevOffset](DetermineTimeZoneAbbrevOffset.md)
  - [ValidateDate](../V/ValidateDate.md)
  - [pg_tzset](../p/pg_tzset.md)
  - [pg_get_timezone_offset](../p/pg_get_timezone_offset.md)
  - [GetCurrentTimeUsec](../G/GetCurrentTimeUsec.md)
  - [GetCurrentDateTime](../G/GetCurrentDateTime.md)
  - [j2date](../j/j2date.md) (Julian to date conversion)
  - [dt2time](../d/dt2time.md) (day time to time components)
  - [time_overflows](../t/time_overflows.md)
- Called from (representative examples):
  - [time_in](../t/time_in.md)
  - [timetz_in](../t/timetz_in.md)

## Notes and Other Information
- Designed specifically for SQL TIME WITH TIME ZONE support, with notable limitations due to SQL standard ambiguities
- Supports both standard and daylight time abbreviations, forcing appropriate GMT offset usage
- Handles Julian date format with optional fractional time components
- Performs extensive field mask validation to prevent duplicate or conflicting time components
- Automatically applies session timezone when no explicit timezone is specified
- The function contains special logic for resolving dynamic timezone abbreviations that require date context
- Returns DTERR error codes for various parsing and validation failures
- Supports both 12-hour (AM/PM) and 24-hour time formats
- Located in src/backend/utils/adt/datetime.c:1864-2397

## Simplified Source

```c
int DecodeTimeOnly(char **field, int *ftype, int nf, int *dtype, struct pg_tm *tm,
                   fsec_t *fsec, int *tzp, DateTimeErrorExtra *extra) {
    int fmask = 0, tmask, type;
    int ptype = 0;  // prefix type for ISO/Julian formats
    int val, dterr;
    bool isjulian = false, is2digits = false, bc = false;
    int mer = HR24;  // AM/PM indicator
    pg_tz *namedTz = NULL, *abbrevTz = NULL;
    char *abbrev = NULL;

    // Initialize time structure
    *dtype = DTK_TIME;
    tm->tm_hour = tm->tm_min = tm->tm_sec = 0;
    *fsec = 0;
    tm->tm_isdst = -1;
    if (tzp != NULL) *tzp = 0;

    // Process each field
    for (int i = 0; i < nf; i++) {
        switch (ftype[i]) {
            case DTK_DATE:
                // Handle date fields for timezone context
                if (tzp == NULL) return DTERR_BAD_FORMAT;
                if (/* limited circumstances for date acceptance */) {
                    dterr = DecodeDate(field[i], fmask, &tmask, &is2digits, tm);
                    if (dterr) return dterr;
                } else {
                    // Handle timezone in date field or named timezone
                    if (isdigit(*field[i])) {
                        // Extract timezone from numeric field
                        char *cp = strchr(field[i], '-');
                        dterr = DecodeTimezone(cp, tzp);
                        if (dterr) return dterr;
                        // Parse remaining as time
                        dterr = DecodeNumberField(strlen(field[i]), field[i],
                                                (fmask | DTK_DATE_M), &tmask, tm, fsec, &is2digits);
                        if (dterr < 0) return dterr;
                    } else {
                        // Named timezone
                        namedTz = pg_tzset(field[i]);
                        if (!namedTz) return DTERR_BAD_TIMEZONE;
                    }
                }
                break;

            case DTK_TIME:
                // Standard time field
                dterr = DecodeTime(field[i], (fmask | DTK_DATE_M),
                                  INTERVAL_FULL_RANGE, &tmask, tm, fsec);
                if (dterr) return dterr;
                break;

            case DTK_TZ:
                // Timezone offset
                if (tzp == NULL) return DTERR_BAD_FORMAT;
                dterr = DecodeTimezone(field[i], tzp);
                if (dterr) return dterr;
                break;

            case DTK_NUMBER:
                // Handle various numeric formats
                if (ptype != 0) {
                    // Process based on prefix type (Julian, ISO time)
                    if (ptype == DTK_JULIAN) {
                        // Julian date conversion
                        j2date(val, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
                        isjulian = true;
                    } else if (ptype == DTK_TIME) {
                        // ISO time format
                        dterr = DecodeNumberField(strlen(field[i]), field[i],
                                                (fmask | DTK_DATE_M), &tmask, tm, fsec, &is2digits);
                        if (dterr < 0) return dterr;
                    }
                } else {
                    // Regular number processing - date or time concatenated format
                    dterr = DecodeNumberField(strlen(field[i]), field[i],
                                            (fmask | DTK_DATE_M), &tmask, tm, fsec, &is2digits);
                    if (dterr < 0) return dterr;
                }
                break;

            case DTK_STRING:
            case DTK_SPECIAL:
                // Handle timezone abbreviations and special keywords
                dterr = DecodeTimezoneAbbrev(i, field[i], &type, &val, &abbrevTz, extra);
                if (dterr) return dterr;
                if (type == UNKNOWN_FIELD)
                    type = DecodeSpecial(i, field[i], &val);

                switch (type) {
                    case RESERV:
                        // Handle 'now', 'zulu' keywords
                        if (val == DTK_NOW) {
                            GetCurrentTimeUsec(tm, fsec, NULL);
                        } else if (val == DTK_ZULU) {
                            tm->tm_hour = tm->tm_min = tm->tm_sec = 0;
                            tm->tm_isdst = 0;
                        }
                        break;
                    case TZ:
                    case DTZ:
                        // Static timezone
                        if (tzp == NULL) return DTERR_BAD_FORMAT;
                        *tzp = -val;
                        tm->tm_isdst = (type == DTZ);
                        break;
                    case DYNTZ:
                        // Dynamic timezone abbreviation
                        abbrevTz = abbrevTz;
                        abbrev = field[i];
                        break;
                    case AMPM:
                        mer = val;
                        break;
                    case UNKNOWN_FIELD:
                        // Try as named timezone
                        namedTz = pg_tzset(field[i]);
                        if (!namedTz) return DTERR_BAD_FORMAT;
                        break;
                }
                break;
        }

        // Check for duplicate field types
        if (tmask & fmask) return DTERR_BAD_FORMAT;
        fmask |= tmask;
    }

    // Validate date components
    dterr = ValidateDate(fmask, isjulian, is2digits, bc, tm);
    if (dterr) return dterr;

    // Handle AM/PM conversion
    if (mer == AM && tm->tm_hour == 12) tm->tm_hour = 0;
    else if (mer == PM && tm->tm_hour != 12) tm->tm_hour += 12;

    // Check time overflow
    if (time_overflows(tm->tm_hour, tm->tm_min, tm->tm_sec, *fsec))
        return DTERR_FIELD_OVERFLOW;

    // Resolve timezone if needed
    if (namedTz != NULL) {
        long int gmtoff;
        if (pg_get_timezone_offset(namedTz, &gmtoff)) {
            *tzp = -(int)gmtoff;
        } else {
            *tzp = DetermineTimeZoneOffset(tm, namedTz);
        }
    }

    if (abbrevTz != NULL) {
        struct pg_tm tt = *tm;
        *tzp = DetermineTimeZoneAbbrevOffset(&tt, abbrev, abbrevTz);
        tm->tm_isdst = tt.tm_isdst;
    }

    // Use session timezone if none specified
    if (tzp != NULL && !(fmask & DTK_M(TZ))) {
        struct pg_tm tt = *tm;
        *tzp = DetermineTimeZoneOffset(&tt, session_timezone);
        tm->tm_isdst = tt.tm_isdst;
    }

    return 0;
}
```