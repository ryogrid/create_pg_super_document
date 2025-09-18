# PrintTiming

## Location
[src/bin/psql/common.c:561-619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L561-L619)

## Overview
PrintTiming is a static helper function in psql that formats and displays elapsed time measurements in a human-readable format, with special handling for different time scales.

## Definition


## Detailed Description
PrintTiming formats and prints execution timing information to stdout. The function takes elapsed time in milliseconds and displays it in different formats depending on the duration:

- For times < 1000ms: Shows traditional format "Time: X.XXX ms"
- For times >= 1 second but < 1 hour: Shows "Time: X.XXX ms (MM:SS.SSS)" format
- For times >= 1 hour but < 1 day: Shows "Time: X.XXX ms (HH:MM:SS.SSS)" format  
- For times >= 1 day: Shows "Time: X.XXX ms (D d HH:MM:SS.SSS)" format

The function always displays the raw millisecond value followed by a breakdown into larger time units when appropriate. This provides both precise timing data and intuitive understanding of longer durations.

## Parameters / Member Variables
- : The elapsed time in milliseconds to be formatted and displayed

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function)
  - floor (math library function)
  - _ (gettext macro for internationalization)

- Called from (representative examples):
  - [PSQLexecWatch](PSQLexecWatch.md)
  - [SendQuery](../S/SendQuery.md)

## Notes and Other Information
- This is a static function, only accessible within src/bin/psql/common.c
- The function uses floor() to ensure proper time unit calculations
- Time formatting includes leading zeros for consistent display alignment
- The pre-v10 format (just milliseconds) is preserved for sub-second timings
- Uses gettext (_) macro for internationalized output strings
- The function prioritizes readability by always showing minutes/seconds format even for short durations over 1 second