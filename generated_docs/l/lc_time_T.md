# lc_time_T

## Location
[src/timezone/strftime.c:48-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/strftime.c#L48-L61)

## Overview
A structure that holds locale-specific time formatting information used by PostgreSQL's strftime implementation for date and time string formatting.

## Definition

```c
struct lc_time_T
{
	const char *mon[MONSPERYEAR];
	const char *month[MONSPERYEAR];
	const char *wday[DAYSPERWEEK];
	const char *weekday[DAYSPERWEEK];
	const char *X_fmt;
	const char *x_fmt;
	const char *c_fmt;
	const char *am;
	const char *pm;
	const char *date_fmt;
};
```
## Detailed Description
The  structure serves as a locale-specific time formatting template for PostgreSQL's date and time string conversion functionality. It encapsulates all the textual and formatting elements needed to represent dates and times in a human-readable format according to locale conventions. This structure is primarily used by the  function to format timestamps into strings using various format specifiers.

The structure is designed to be a complete locale definition containing abbreviated and full names for months and weekdays, along with time formatting patterns and AM/PM indicators. In PostgreSQL's implementation, this structure is instantiated as  which provides the default C locale formatting behavior.

## Parameters / Member Variables
- : Array of 12 abbreviated month names ("Jan", "Feb", ..., "Dec")
- : Array of 12 full month names ("January", "February", ..., "December")
- : Array of 7 abbreviated weekday names ("Sun", "Mon", ..., "Sat")
- : Array of 7 full weekday names ("Sunday", "Monday", ..., "Saturday")
- : Default time format string ("%H:%M:%S")
- : Default date format string ("%m/%d/%y") 
- : Default date and time format string ("%a %b %e %T %Y")
- : Morning time indicator string ("AM")
- : Evening time indicator string ("PM")
- : Complete date format string ("%a %b %e %H:%M:%S %Z %Y")

## Dependencies
- Functions called/Symbols referenced:
  - MONSPERYEAR (defined as 12)
  - DAYSPERWEEK (defined as 7)
- Called from (representative examples):
  - Locale macro (defined as &C_time_locale)
  - Used indirectly by pg_strftime and _fmt functions

## Notes and Other Information
This structure is part of PostgreSQL's timezone handling system, located in src/timezone/strftime.c. It follows the C locale conventions and is used as the default locale for time formatting operations. The structure definition closely mirrors the standard C library's locale time formatting structure but is specifically adapted for PostgreSQL's needs. The constants MONSPERYEAR and DAYSPERWEEK are defined in src/timezone/private.h as 12 and 7 respectively, ensuring proper array sizing for month and weekday name arrays.