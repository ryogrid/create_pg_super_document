# rulesub

## Location
[src/timezone/zic.c:1823-1991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1823-L1991)

## Overview
Parses and validates individual timezone rule components including years, months, days, and time specifications for daylight saving time transitions.

## Definition

```c
static void
rulesub(struct rule *rp, const char *loyearp, const char *hiyearp,
		const char *typep, const char *monthp, const char *dayp,
		const char *timep)
```
## Detailed Description
The  function is a core component of PostgreSQL's timezone compiler () responsible for parsing and validating the complex components of timezone rules. It processes the various fields that define when daylight saving time transitions occur, including year ranges, month names, day specifications (which can be complex expressions like "last Sunday" or "Sun>=7"), and time-of-day specifications with timezone indicators.

The function performs extensive validation and parsing of each component, handling special keywords like "minimum", "maximum", and "only" for years, parsing month names, interpreting complex day-of-month expressions including weekday-relative specifications, and parsing time specifications with optional timezone suffixes (s/w/g/u/z for standard/wall/Greenwich/Universal/Zulu time).

All parsed information is stored in the provided rule structure for later use in generating the compiled timezone data.

## Parameters / Member Variables
- : Pointer to rule structure to populate with parsed information
- : String specifying the starting year (can be numeric or keyword like "minimum")
- : String specifying the ending year (can be numeric, keyword, or "only")
- : Year type specification (must be empty string in modern usage)
- : Month name (e.g., "Jan", "February") to be looked up in month name table
- : Day specification (can be numeric, "lastSunday", "Sun>=7", "Sun<=20", etc.)
- : Time specification with optional suffix (e.g., "2:00", "2:00s", "2:00w")

## Dependencies
- Functions called/Symbols referenced:
  - byword (to lookup month names, year keywords, weekday names)
  - ecpyalloc (to allocate temporary string copies for parsing)
  - lowerit (to normalize case for time suffix parsing)
  - gethms (to parse hour:minute:second time specifications)
  - error (for reporting parsing errors)
  - free (to deallocate temporary strings)
  - sscanf, strlen, strchr (standard string processing functions)
  - fprintf, exit (for fatal error handling)
- Called from (representative examples):
  - inrule (when processing Rule lines)
  - inzsub (when processing Zone continuation lines)

## Notes and Other Information
- This function handles the most complex parsing in the timezone compiler, dealing with various date/time specification formats
- Day specifications support multiple formats: numeric day-of-month, "last" + weekday, weekday + comparison operator + day
- Time suffixes indicate the timezone context: 's' = standard time, 'w' = wall clock time, 'g'/'u'/'z' = UTC variants
- Year ranges can span from ZIC_MIN to ZIC_MAX, handling both numeric years and special keywords
- The function validates that start years don't exceed end years and that day-of-month values are valid for the specified month
- Extensive error checking ensures malformed rule specifications are caught during compilation rather than causing runtime issues
- Year type specifications are deprecated and must be empty in modern timezone database files