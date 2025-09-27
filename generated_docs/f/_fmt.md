# _fmt

## Location
[src/timezone/strftime.c:151-515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/strftime.c#L151-L515)

## Overview
Core internal formatting function that processes strftime format specifiers and converts timestamp components into their string representations according to POSIX and C99 standards.

## Definition

```c
static char *
_fmt(const char *format, const struct pg_tm *t, char *pt,
	 const char *ptlim, enum warn *warnp)
```
## Detailed Description
_fmt is the heart of PostgreSQL's strftime implementation, responsible for parsing format strings and converting timestamp components into formatted output. It processes each character in the format string, handling literal characters by copying them directly and format specifiers (prefixed with %) by converting the corresponding timestamp field using appropriate formatting.

The function implements a comprehensive set of format specifiers including POSIX standard ones (%Y, %m, %d, etc.) and extensions (%F for ISO date, %T for time, etc.). It handles complex cases like ISO 8601 week numbering (%V, %G, %g) and timezone formatting (%z, %Z). The function is recursive, as some format specifiers expand to other format strings that are processed by calling _fmt again.

Error handling includes bounds checking to prevent buffer overflows and warning propagation for potentially problematic conversions like 2-digit years. The function maintains locale awareness through the global Locale structure for month names, day names, and other locale-specific formatting.

## Parameters / Member Variables
- : Format string containing literal text and % format specifiers
- : Pointer to pg_tm structure containing the timestamp components to format
- : Current position in the output buffer where formatted text should be written
- : Pointer to the end of the output buffer (exclusive limit)
- : Pointer to warning level that tracks potential formatting issues

## Dependencies
- Functions called/Symbols referenced:
  - [_add](../a/_add.md) (adds strings to output buffer)
  - [_conv](../c/_conv.md) (converts integers to formatted strings)
  - [_yconv](../y/_yconv.md) (converts years with century handling)
  - isleap_sum (checks for leap years)
  - Locale (global locale information structure)
  - Various constants: DAYSPERWEEK, MONSPERYEAR, TM_YEAR_BASE, etc.
- Called from (representative examples):
  - [pg_strftime](../p/pg_strftime.md) (main entry point)
  - [_fmt](_fmt.md) (recursive calls for composite format specifiers)

## Notes and Other Information
- Supports extensive format specifiers including POSIX standard and common extensions
- Handles ISO 8601 week date calculations (%V, %G, %g) with complex logic for year boundaries
- Implements locale-aware formatting for month/day names and AM/PM indicators
- Uses recursive approach for composite format specifiers like %c, %D, %F, %R, %r, %T
- Includes historical compatibility notes and comments about format specifier changes
- Contains optional KITCHEN_SINK feature for %K specifier (humorous Easter egg)
- Warning system tracks potentially problematic conversions like 2-digit years
- Buffer overflow protection through ptlim boundary checking

## Simplified Source

```c
// Simplified version of _fmt
static char *_fmt(const char *format, const struct pg_tm *t, char *pt, const char *ptlim, enum warn *warnp) {
    // Main formatting loop: process each character in format string
    for (; *format; ++format) {
        if (*format == '%') {
            // Handle format specifiers
            switch (*++format) {
                case '\0':
                    --format;  // Handle trailing %
                    break;

                // Basic date/time components
                case 'Y':  // 4-digit year
                    pt = _yconv(t->tm_year, TM_YEAR_BASE, true, true, pt, ptlim);
                    continue;
                case 'm':  // Month (01-12)
                    pt = _conv(t->tm_mon + 1, "%02d", pt, ptlim);
                    continue;
                case 'd':  // Day of month (01-31)
                    pt = _conv(t->tm_mday, "%02d", pt, ptlim);
                    continue;
                case 'H':  // Hour 24-hour format (00-23)
                    pt = _conv(t->tm_hour, "%02d", pt, ptlim);
                    continue;
                case 'M':  // Minutes (00-59)
                    pt = _conv(t->tm_min, "%02d", pt, ptlim);
                    continue;
                case 'S':  // Seconds (00-59)
                    pt = _conv(t->tm_sec, "%02d", pt, ptlim);
                    continue;

                // Day/month names (locale-aware)
                case 'A':  // Full weekday name
                    pt = _add((t->tm_wday < 0 || t->tm_wday >= DAYSPERWEEK) ?
                             "?" : Locale->weekday[t->tm_wday], pt, ptlim);
                    continue;
                case 'B':  // Full month name
                    pt = _add((t->tm_mon < 0 || t->tm_mon >= MONSPERYEAR) ?
                             "?" : Locale->month[t->tm_mon], pt, ptlim);
                    continue;

                // Composite formats (recursive)
                case 'F':  // ISO date format (YYYY-MM-DD)
                    pt = _fmt("%Y-%m-%d", t, pt, ptlim, warnp);
                    continue;
                case 'T':  // ISO time format (HH:MM:SS)
                    pt = _fmt("%H:%M:%S", t, pt, ptlim, warnp);
                    continue;
                case 'c':  // Complete date/time representation
                    pt = _fmt(Locale->c_fmt, t, pt, ptlim, warnp);
                    continue;

                // Timezone handling
                case 'Z':  // Timezone abbreviation
                    if (t->tm_zone != NULL) {
                        pt = _add(t->tm_zone, pt, ptlim);
                    }
                    continue;
                case 'z':  // Timezone offset (+HHMM)
                    // Calculate and format GMT offset
                    if (t->tm_isdst >= 0) {
                        long diff = t->tm_gmtoff;
                        bool negative = diff < 0;
                        if (negative) diff = -diff;

                        pt = _add(negative ? "-" : "+", pt, ptlim);
                        diff /= SECSPERMIN;
                        diff = (diff / MINSPERHOUR) * 100 + (diff % MINSPERHOUR);
                        pt = _conv(diff, "%04d", pt, ptlim);
                    }
                    continue;

                // ISO 8601 week date (complex calculation)
                case 'V':  // Week number
                case 'G':  // ISO year (4 digits)
                case 'g':  // ISO year (2 digits)
                    // Simplified: delegate to ISO week calculation logic
                    pt = calculate_iso_week_format(*format, t, pt, ptlim, warnp);
                    continue;

                // Special characters
                case 'n':  // Newline
                    pt = _add("\n", pt, ptlim);
                    continue;
                case 't':  // Tab
                    pt = _add("\t", pt, ptlim);
                    continue;
                case '%':  // Literal %
                default:   // Unknown specifier - print as-is
                    break;
            }
        }

        // Check buffer bounds
        if (pt == ptlim) break;

        // Copy literal character
        *pt++ = *format;
    }

    return pt;
}
```

Key simplifications made:
- Condensed the massive switch statement to show core format specifiers
- Grouped related functionality (dates, times, names, etc.)
- Abstracted complex ISO 8601 week logic into a helper function concept
- Maintained the essential structure: format parsing, specifier handling, literal copying
- Preserved buffer overflow protection and recursive formatting calls
- Simplified timezone handling while showing the basic approach
- Removed extensive comments and historical notes for clarity
- Focused on the most commonly used format specifiers