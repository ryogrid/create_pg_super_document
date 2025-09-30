# EncodeInterval

## Location
[src/backend/utils/adt/datetime.c:4585-4778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L4585-L4778)

## Overview
Converts a PostgreSQL interval structure to its string representation, supporting multiple output formats including SQL Standard, ISO 8601, traditional PostgreSQL, and verbose PostgreSQL styles.

## Definition
```c
void EncodeInterval(struct pg_itm *itm, int style, char *str)
```

## Detailed Description
This function is the primary interface for converting PostgreSQL interval data structures to human-readable string representations. It supports four different output formats: SQL Standard format (with strict sign handling), ISO 8601 duration format (P1Y2M3DT4H5M6S style), traditional PostgreSQL format (compatible with versions < 8.4), and verbose PostgreSQL format (using full words like 'years', 'months', etc.). The function handles complex sign logic, zero value suppression, and format-specific requirements for each output style.

## Parameters / Member Variables
- `itm`: Pointer to a pg_itm structure containing the interval components (years, months, days, hours, minutes, seconds, microseconds)
- `style`: Integer constant specifying the desired output format (INTSTYLE_SQL_STANDARD, INTSTYLE_ISO_8601, INTSTYLE_POSTGRES, or INTSTYLE_POSTGRES_VERBOSE)
- `str`: Output buffer where the formatted interval string will be written

## Dependencies
- Functions called/Symbols referenced:
  - [AddPostgresIntPart](../A/AddPostgresIntPart.md) (for PostgreSQL format output)
  - [AddVerboseIntPart](../A/AddVerboseIntPart.md) (for verbose PostgreSQL format output)
  - [AddISO8601IntPart](../A/AddISO8601IntPart.md) (for ISO 8601 format output)
  - [AppendSeconds](../A/AppendSeconds.md) (for formatting seconds with microseconds)
  - i64abs (for 64-bit absolute value calculations)
  - sprintf, strcpy, strcat (standard C string functions)
- Called from (representative examples):
  - [interval_out](../i/interval_out.md) (in src/backend/utils/adt/timestamp.c)
  - [PGTYPESinterval_to_asc](../P/PGTYPESinterval_to_asc.md) (in src/interfaces/ecpg/pgtypeslib/interval.c)

## Notes and Other Information
- Handles complex sign logic differently for each format style, with SQL Standard requiring single leading signs, ISO 8601 using individual field signs, and PostgreSQL styles using contextual sign handling
- Special handling for zero intervals to ensure meaningful output in all formats
- The function modifies local copies of interval components to handle sign normalization without affecting the input structure
- Supports backward compatibility with older PostgreSQL versions through the INTSTYLE_POSTGRES format
- Uses format-specific helper functions to maintain clean separation of formatting logic
- Part of PostgreSQL's core interval data type system, essential for interval output operations

## Simplified Source

```c
void EncodeInterval(struct pg_itm *itm, int style, char *str)
{
    char *cp = str;
    int year = itm->tm_year;
    int mon = itm->tm_mon;
    int64 mday = itm->tm_mday;
    int64 hour = itm->tm_hour;
    int min = itm->tm_min;
    int sec = itm->tm_sec;
    int fsec = itm->tm_usec;
    bool is_before = false;
    bool is_zero = true;

    switch (style) {
        case INTSTYLE_SQL_STANDARD:
            {
                bool has_negative = year < 0 || mon < 0 || mday < 0 || hour < 0 || min < 0 || sec < 0 || fsec < 0;
                bool has_positive = year > 0 || mon > 0 || mday > 0 || hour > 0 || min > 0 || sec > 0 || fsec > 0;
                bool has_year_month = year != 0 || mon != 0;
                bool has_day_time = mday != 0 || hour != 0 || min != 0 || sec != 0 || fsec != 0;
                bool sql_standard_value = !(has_negative && has_positive) && !(has_year_month && has_day_time);

                // Apply global negative sign for SQL standard format
                if (has_negative && sql_standard_value) {
                    *cp++ = '-';
                    year = -year; mon = -mon; mday = -mday;
                    hour = -hour; min = -min; sec = -sec; fsec = -fsec;
                }

                // Format based on components present
                if (!has_negative && !has_positive) {
                    sprintf(cp, "0");
                } else if (has_year_month) {
                    sprintf(cp, "%d-%d", year, mon);
                } else if (has_day_time) {
                    sprintf(cp, "%lld %lld:%02d:", (long long) mday, (long long) hour, min);
                    cp += strlen(cp);
                    cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
                    *cp = '\0';
                }
            }
            break;

        case INTSTYLE_ISO_8601:
            // ISO 8601 format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
            if (year == 0 && mon == 0 && mday == 0 && hour == 0 && min == 0 && sec == 0 && fsec == 0) {
                sprintf(cp, "PT0S");
                break;
            }
            *cp++ = 'P';
            cp = AddISO8601IntPart(cp, year, 'Y');
            cp = AddISO8601IntPart(cp, mon, 'M');
            cp = AddISO8601IntPart(cp, mday, 'D');
            if (hour != 0 || min != 0 || sec != 0 || fsec != 0)
                *cp++ = 'T';
            cp = AddISO8601IntPart(cp, hour, 'H');
            cp = AddISO8601IntPart(cp, min, 'M');
            if (sec != 0 || fsec != 0) {
                if (sec < 0 || fsec < 0)
                    *cp++ = '-';
                cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, false);
                *cp++ = 'S';
                *cp++ = '\0';
            }
            break;

        case INTSTYLE_POSTGRES:
            // Traditional PostgreSQL format
            cp = AddPostgresIntPart(cp, year, "year", &is_zero, &is_before);
            cp = AddPostgresIntPart(cp, mon, "mon", &is_zero, &is_before);
            cp = AddPostgresIntPart(cp, mday, "day", &is_zero, &is_before);
            if (is_zero || hour != 0 || min != 0 || sec != 0 || fsec != 0) {
                bool minus = (hour < 0 || min < 0 || sec < 0 || fsec < 0);
                sprintf(cp, "%s%s%02lld:%02d:",
                        is_zero ? "" : " ",
                        (minus ? "-" : (is_before ? "+" : "")),
                        (long long) i64abs(hour), abs(min));
                cp += strlen(cp);
                cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
                *cp = '\0';
            }
            break;

        case INTSTYLE_POSTGRES_VERBOSE:
        default:
            // Verbose PostgreSQL format with full words
            strcpy(cp, "@");
            cp++;
            cp = AddVerboseIntPart(cp, year, "year", &is_zero, &is_before);
            cp = AddVerboseIntPart(cp, mon, "mon", &is_zero, &is_before);
            cp = AddVerboseIntPart(cp, mday, "day", &is_zero, &is_before);
            cp = AddVerboseIntPart(cp, hour, "hour", &is_zero, &is_before);
            cp = AddVerboseIntPart(cp, min, "min", &is_zero, &is_before);
            if (sec != 0 || fsec != 0) {
                *cp++ = ' ';
                if (sec < 0 || (sec == 0 && fsec < 0)) {
                    if (is_zero) is_before = true;
                    else if (!is_before) *cp++ = '-';
                } else if (is_before) {
                    *cp++ = '-';
                }
                cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, false);
                sprintf(cp, " sec%s", (abs(sec) != 1 || fsec != 0) ? "s" : "");
                is_zero = false;
            }
            if (is_zero) strcat(cp, " 0");
            if (is_before) strcat(cp, " ago");
            break;
    }
}
```