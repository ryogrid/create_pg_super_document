# getrule

## Location
[src/timezone/localtime.c:778-838](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L778-L838)

## Overview
Extracts a timezone rule in the POSIX format date[/time] from a timezone string, supporting Julian days, month/week/day specifications, and day-of-year formats.

## Definition
```c
static const char *getrule(const char *strp, struct rule *const rulep)
```

## Detailed Description
The getrule function parses POSIX timezone rules that specify when daylight saving time transitions occur. It supports three different date specification formats:

1. **Julian Day (Jn)**: Julian day number (1-365), where leap day is never counted
2. **Month/Week/Day (Mm.w.d)**: Month (1-12), week (1-5), day of week (0-6, where 0=Sunday)
3. **Day of Year (n)**: Zero-based day of year (0-365), where leap day is counted

After parsing the date specification, the function optionally parses a time specification following a '/' character. If no time is specified, it defaults to 2:00:00 AM. The parsed rule is stored in the provided rule structure with the appropriate type, date components, and transition time.

## Parameters / Member Variables
- `strp`: Pointer to the timezone string to parse, positioned at the start of the rule specification
- `rulep`: Pointer to a struct rule where the parsed rule information will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [getnum](getnum.md) (for parsing numeric components)
  - [getoffset](getoffset.md) (for parsing optional time specification)
  - is_digit (for checking if character is a digit)
  - JULIAN_DAY, MONTH_NTH_DAY_OF_WEEK, DAY_OF_YEAR (rule type constants)
  - DAYSPERNYEAR, MONSPERYEAR, DAYSPERWEEK, DAYSPERLYEAR (time constants)
  - SECSPERHOUR (for default time calculation)
- Called from (representative examples):
  - [tzparse](../t/tzparse.md)

## Notes and Other Information
- This is a static function used internally within the timezone parsing subsystem
- The function implements POSIX section 8 timezone rule format specifications
- Default transition time is 2:00:00 AM when no time is explicitly specified
- Julian day format excludes leap days, while day-of-year format includes them
- Month/week/day format allows specifying rules like "first Sunday in April" or "last Sunday in October"
- Week 5 in month/week/day format means "last occurrence" of the specified weekday
- Returns NULL if the rule format is invalid or parsing fails at any point
- The parsed rule structure contains all necessary information for calculating actual transition dates

## Simplified Source

```c
// Simplified version of getrule - parses POSIX timezone rule formats
static const char *getrule(const char *strp, struct rule *const rulep) {
    // Parse different date specification formats
    if (*strp == 'J') {
        // Julian day format: J1-365 (no leap day)
        rulep->r_type = JULIAN_DAY;
        strp = getnum(++strp, &rulep->r_day, 1, DAYSPERNYEAR);
    }
    else if (*strp == 'M') {
        // Month/week/day format: Mm.w.d (e.g., M3.2.0 = 2nd Sunday in March)
        rulep->r_type = MONTH_NTH_DAY_OF_WEEK;
        strp = getnum(++strp, &rulep->r_mon, 1, MONSPERYEAR);
        if (!strp || *strp++ != '.') return NULL;

        strp = getnum(strp, &rulep->r_week, 1, 5);
        if (!strp || *strp++ != '.') return NULL;

        strp = getnum(strp, &rulep->r_day, 0, DAYSPERWEEK - 1);
    }
    else if (is_digit(*strp)) {
        // Day of year format: 0-365 (includes leap day)
        rulep->r_type = DAY_OF_YEAR;
        strp = getnum(strp, &rulep->r_day, 0, DAYSPERLYEAR - 1);
    }
    else {
        return NULL;  // Invalid format
    }

    if (!strp) return NULL;

    // Parse optional time specification after '/'
    if (*strp == '/') {
        strp = getoffset(++strp, &rulep->r_time);
    }
    else {
        rulep->r_time = 2 * SECSPERHOUR;  // Default: 2:00:00 AM
    }

    return strp;
}
```

Key simplifications made:
- Consolidated error checking patterns for cleaner flow
- Combined increment and assignment operations (++strp)
- Used more descriptive comments explaining each format type
- Simplified conditional logic while preserving all functionality
- Maintained the three distinct parsing paths for different rule formats
- Preserved essential error handling for malformed input