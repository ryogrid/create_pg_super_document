# inleap

## Location
[src/timezone/zic.c:1755-1786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1755-L1786)

## Overview
Processes leap second information from timezone database files, parsing and validating leap second correction entries.

## Definition

```c
struct lookup const *lp = byword(fields[LP_ROLL], leap_types);
```
## Detailed Description
The  function is a core component of PostgreSQL's timezone compiler () that handles leap second entries from timezone database files. It parses and validates leap second data, including the datetime when the leap second occurs, the type of correction (rolling or stationary), and the direction of the correction (+1 or -1 second).

The function performs comprehensive validation of the leap second entry format, ensuring the correct number of fields are present, validating the datetime specification, checking the rolling/stationary field against known types, and parsing the correction field to determine if a second should be added or subtracted.

If all validations pass, it calls  to actually record the leap second information for use in timezone calculations.

## Parameters / Member Variables
- : Array of string fields parsed from the leap second line in the timezone database file
- : Number of fields in the fields array, must match LEAP_FIELDS for valid leap entries

## Dependencies
- Functions called/Symbols referenced:
  - error (for reporting parsing errors)
  - getleapdatetime (to parse and validate the leap second datetime)
  - byword (to lookup and validate the rolling/stationary field)
  - leapadd (to actually add the leap second to the timezone data)
  - LEAP_FIELDS (constant defining expected number of fields)
  - LP_ROLL, LP_CORR (field index constants)
  - zic_t (time type used for leap second timestamps)
  - lookup (struct type for field lookups)
- Called from (representative examples):
  - infile (main file parsing function)

## Notes and Other Information
- This function is part of PostgreSQL's timezone data compilation system, not the runtime timezone handling
- Leap seconds are rare corrections to UTC that account for variations in Earth's rotation
- The function handles both positive (+1 second) and negative (-1 second) leap second corrections
- Input validation is strict - any malformed leap second entry will result in an error and compilation failure
- The rolling/stationary field determines how the leap second affects time zone transitions