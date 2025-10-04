# PGTYPEStimestamp_defmt_scan

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:2519-3010](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L2519-L3010)

## Overview
A comprehensive date/time format string parser that converts formatted string input into PostgreSQL timestamp values according to strftime-style format specifiers.

## Definition
```c
int PGTYPEStimestamp_defmt_scan(char **str, char *fmt, timestamp *d,
                               int *year, int *month, int *day,
                               int *hour, int *minute, int *second,
                               int *tz)
```

## Detailed Description
This function is the core date/time parsing engine for the ECPG pgtypes library. It interprets a wide variety of strftime-style format specifiers to parse formatted date and time strings into their component values. The function supports extensive format codes including:

- Date components: %a/%A (weekday names), %b/%B/%h (month names), %C (century), %d/%e (day), %m (month), %y/%g/%G/%Y (year variations)
- Time components: %H/%I/%k/%l (hour variations), %M (minute), %S (second), %p/%P (AM/PM indicators)
- Special formats: %D (MM/DD/YY), %r (12-hour time), %R (24-hour time), %T (time), %s (Unix timestamp)
- Week and timezone: %j (day of year), %u/%U/%V/%w/%W (week-related), %z/%Z (timezone)
- Literals: %n (newline), %t (tab), %% (percent sign)

The function recursively handles composite format specifiers (like %D, %r, %R, %T) by expanding them into their constituent parts. It performs comprehensive validation of parsed values and constructs a final timestamp using tm2timestamp().

## Parameters / Member Variables
- `str`: Pointer to input string pointer (modified to track parsing position)
- `fmt`: Format string with strftime-style specifiers defining expected input structure
- `d`: Output timestamp value constructed from parsed components
- `year`: Pointer to store parsed year value
- `month`: Pointer to store parsed month value (1-12)
- `day`: Pointer to store parsed day value (1-31)
- `hour`: Pointer to store parsed hour value (0-24)
- `minute`: Pointer to store parsed minute value (0-59)
- `second`: Pointer to store parsed second value (0-59)
- `tz`: Pointer to store parsed timezone offset in seconds

## Dependencies
- Functions called/Symbols referenced:
  - [pgtypes_defmt_scan](../p/pgtypes_defmt_scan.md) (extensively for individual component parsing)
  - [pgtypes_alloc](../p/pgtypes_alloc.md) (for temporary string allocation)
  - [DecodeTimezone](../D/DecodeTimezone.md) (for timezone string parsing)
  - [tm2timestamp](../t/tm2timestamp.md) (for final timestamp construction)
  - strncmp (standard C library function)
  - strlen (standard C library function)
  - strcpy/strcat (standard C library functions)
  - gmtime (standard C library function)
  - free (standard C library function)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (PostgreSQL string comparison)
  - isleap (leap year checking)
  - Various constants: PGTYPES_TYPE_UINT, PGTYPES_TYPE_UINT_LONG, PGTYPES_TYPE_STRING_MALLOCED, MONTHS_PER_YEAR, TZ, DTZ
  - Global arrays: pgtypes_date_weekdays_short, days, months, pgtypes_date_months, datetktbl, day_tab
- Called from (representative examples):
  - [PGTYPEStimestamp_defmt_asc](PGTYPEStimestamp_defmt_asc.md)
  - Self-recursively for composite format specifiers

## Notes and Other Information
- Returns 0 on success, 1 on error
- Handles whitespace flexibly by skipping spaces in both input and format strings
- Supports both short and long forms of weekday/month names
- Implements AM/PM time conversion with multiple formats (am/pm, a.m./p.m., AM/PM, A.M./P.M.)
- Validates all parsed values against reasonable ranges and adjusts invalid values
- Uses recursive parsing for composite format specifiers like %D, %r, %R, %T
- Handles 2-digit year interpretation (adds 1900 if year < 100)
- Part of the ECPG pgtypes library for embedded SQL date/time processing
- Located in src/interfaces/ecpg/pgtypeslib/dt_common.c:2519-3010
- Contains XXX comments indicating areas for potential future enhancement
- Supports Unix timestamp parsing via %s format specifier

## Simplified Source

```c
int PGTYPEStimestamp_defmt_scan(char **str, char *fmt, timestamp *d,
                               int *year, int *month, int *day,
                               int *hour, int *minute, int *second,
                               int *tz) {
    union un_fmt_comb scan_val;
    int scan_type;
    char *pstr = *str, *pfmt = fmt, *tmp;
    int err = 1;
    unsigned int j;
    struct tm tm;

    // Main parsing loop
    while (*pfmt) {
        err = 0;

        // Skip whitespace in both format and input strings
        while (*pfmt == ' ') pfmt++;
        while (*pstr == ' ') pstr++;

        // Handle literal characters (non-format specifiers)
        if (*pfmt != '%') {
            if (*pfmt == *pstr) {
                pfmt++;
                pstr++;
            } else {
                return 1; // Error: no match
            }
            continue;
        }

        // Process format specifiers
        pfmt++; // Skip '%'
        switch (*pfmt) {
            case 'a': // Short weekday names
                pfmt++;
                err = 1;
                for (j = 0; pgtypes_date_weekdays_short[j]; j++) {
                    if (strncmp(pgtypes_date_weekdays_short[j], pstr,
                               strlen(pgtypes_date_weekdays_short[j])) == 0) {
                        err = 0;
                        pstr += strlen(pgtypes_date_weekdays_short[j]);
                        break;
                    }
                }
                break;

            case 'A': // Full weekday names
                pfmt++;
                err = 1;
                for (j = 0; days[j]; j++) {
                    if (strncmp(days[j], pstr, strlen(days[j])) == 0) {
                        err = 0;
                        pstr += strlen(days[j]);
                        break;
                    }
                }
                break;

            case 'b': case 'h': // Short month names
                pfmt++;
                err = 1;
                for (j = 0; months[j]; j++) {
                    if (strncmp(months[j], pstr, strlen(months[j])) == 0) {
                        err = 0;
                        pstr += strlen(months[j]);
                        *month = j + 1;
                        break;
                    }
                }
                break;

            case 'B': // Full month names
                pfmt++;
                err = 1;
                for (j = 0; pgtypes_date_months[j]; j++) {
                    if (strncmp(pgtypes_date_months[j], pstr,
                               strlen(pgtypes_date_months[j])) == 0) {
                        err = 0;
                        pstr += strlen(pgtypes_date_months[j]);
                        *month = j + 1;
                        break;
                    }
                }
                break;

            case 'C': // Century
                pfmt++;
                scan_type = PGTYPES_TYPE_UINT;
                err = pgtypes_defmt_scan(&scan_val, scan_type, &pstr, pfmt);
                *year = scan_val.uint_val * 100;
                break;

            case 'd': case 'e': // Day of month
                pfmt++;
                scan_type = PGTYPES_TYPE_UINT;
                err = pgtypes_defmt_scan(&scan_val, scan_type, &pstr, pfmt);
                *day = scan_val.uint_val;
                break;

            case 'D': // Date format MM/DD/YY
                pfmt++;
                tmp = pgtypes_alloc(strlen("%m/%d/%y") + strlen(pstr) + 1);
                if (!tmp) return 1;
                strcpy(tmp, "%m/%d/%y");
                strcat(tmp, pfmt);
                err = PGTYPEStimestamp_defmt_scan(&pstr, tmp, d, year, month, day, hour, minute, second, tz);
                free(tmp);
                return err;

            case 'm': // Month number
                pfmt++;
                scan_type = PGTYPES_TYPE_UINT;
                err = pgtypes_defmt_scan(&scan_val, scan_type, &pstr, pfmt);
                *month = scan_val.uint_val;
                break;

            case 'y': case 'g': // 2-digit year
                pfmt++;
                scan_type = PGTYPES_TYPE_UINT;
                err = pgtypes_defmt_scan(&scan_val, scan_type, &pstr, pfmt);
                if (*year < 0) {
                    *year = scan_val.uint_val;
                } else {
                    *year += scan_val.uint_val;
                }
                if (*year < 100) *year += 1900;
                break;

            case 'Y': // 4-digit year
                pfmt++;
                scan_type = PGTYPES_TYPE_UINT;
                err = pgtypes_defmt_scan(&scan_val, scan_type, &pstr, pfmt);
                *year = scan_val.uint_val;
                break;

            case 'H': case 'I': case 'k': case 'l': // Hour formats
                pfmt++;
                scan_type = PGTYPES_TYPE_UINT;
                err = pgtypes_defmt_scan(&scan_val, scan_type, &pstr, pfmt);
                *hour += scan_val.uint_val;
                break;

            case 'M': // Minutes
                pfmt++;
                scan_type = PGTYPES_TYPE_UINT;
                err = pgtypes_defmt_scan(&scan_val, scan_type, &pstr, pfmt);
                *minute = scan_val.uint_val;
                break;

            case 'S': // Seconds
                pfmt++;
                scan_type = PGTYPES_TYPE_UINT;
                err = pgtypes_defmt_scan(&scan_val, scan_type, &pstr, pfmt);
                *second = scan_val.uint_val;
                break;

            case 'p': // AM/PM lowercase
                err = 1;
                pfmt++;
                if (strncmp(pstr, "am", 2) == 0 || strncmp(pstr, "a.m.", 4) == 0) {
                    err = 0;
                    pstr += (strncmp(pstr, "am", 2) == 0) ? 2 : 4;
                } else if (strncmp(pstr, "pm", 2) == 0 || strncmp(pstr, "p.m.", 4) == 0) {
                    *hour += 12;
                    err = 0;
                    pstr += (strncmp(pstr, "pm", 2) == 0) ? 2 : 4;
                }
                break;

            case 's': // Unix timestamp
                pfmt++;
                scan_type = PGTYPES_TYPE_UINT_LONG;
                err = pgtypes_defmt_scan(&scan_val, scan_type, &pstr, pfmt);
                if (!err) {
                    struct tm *tms;
                    time_t et = (time_t) scan_val.luint_val;
                    tms = gmtime(&et);
                    if (tms) {
                        *year = tms->tm_year + 1900;
                        *month = tms->tm_mon + 1;
                        *day = tms->tm_mday;
                        *hour = tms->tm_hour;
                        *minute = tms->tm_min;
                        *second = tms->tm_sec;
                    } else {
                        err = 1;
                    }
                }
                break;

            case 'z': // Timezone offset
                pfmt++;
                scan_type = PGTYPES_TYPE_STRING_MALLOCED;
                err = pgtypes_defmt_scan(&scan_val, scan_type, &pstr, pfmt);
                if (!err) {
                    err = DecodeTimezone(scan_val.str_val, tz);
                    free(scan_val.str_val);
                }
                break;

            case '%': // Literal percent
                pfmt++;
                if (*pstr == '%') {
                    pstr++;
                } else {
                    err = 1;
                }
                break;

            default:
                err = 1;
        }

        if (err) break;
    }

    // Validate and normalize parsed values
    if (!err) {
        // Set defaults for unspecified values
        if (*second < 0) *second = 0;
        if (*minute < 0) *minute = 0;
        if (*hour < 0) *hour = 0;
        if (*day < 0) { err = 1; *day = 1; }
        if (*month < 0) { err = 1; *month = 1; }
        if (*year < 0) { err = 1; *year = 1970; }

        // Validate ranges
        if (*second > 59) { err = 1; *second = 0; }
        if (*minute > 59) { err = 1; *minute = 0; }
        if (*hour > 24 || (*hour == 24 && (*minute > 0 || *second > 0))) {
            err = 1; *hour = 0;
        }
        if (*month > MONTHS_PER_YEAR) { err = 1; *month = 1; }
        if (*day > day_tab[isleap(*year)][*month - 1]) {
            *day = day_tab[isleap(*year)][*month - 1];
            err = 1;
        }

        // Build final timestamp
        tm.tm_sec = *second;
        tm.tm_min = *minute;
        tm.tm_hour = *hour;
        tm.tm_mday = *day;
        tm.tm_mon = *month;
        tm.tm_year = *year;

        tm2timestamp(&tm, 0, tz, d);
    }

    return err;
}
```