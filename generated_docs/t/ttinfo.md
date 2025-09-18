# ttinfo

## Location
src/timezone/pgtz.h: 26 - 34

## Overview
The ttinfo struct represents time type information for timezone transitions, storing offset data and characteristics for specific timezone states.

## Definition


## Detailed Description
The ttinfo struct defines the properties of a specific timezone state, including its offset from UTC, daylight saving time status, and standardization flags. Each instance represents one possible timezone configuration that can be active during certain time periods. This structure is fundamental to PostgreSQL's timezone handling system, allowing the database to correctly interpret and convert timestamps across different timezone rules and historical changes.

## Parameters / Member Variables
- : The offset from UTC in seconds (positive for east of UTC, negative for west)
- : Boolean flag indicating whether this time type represents daylight saving time
- : Index into the abbreviation character array for the timezone abbreviation (e.g., "EST", "EDT")
- : Boolean flag indicating whether the transition time is in standard time
- : Boolean flag indicating whether the transition time is in universal time

## Dependencies
- Functions called/Symbols referenced: None (pure data structure)
- Called from (representative examples):
  - init_ttinfo (initialization function)
  - tzloadbody (timezone data loading)
  - typesequiv (timezone type comparison)
  - localsub (local time calculation)
  - pg_next_dst_boundary (DST boundary calculation)
  - [pg_interpret_timezone_abbrev](../p/pg_interpret_timezone_abbrev.md) (timezone abbreviation interpretation)

## Notes and Other Information
This structure is part of PostgreSQL's timezone library integration and is used internally to represent the various timezone states that can occur throughout a year. The abbreviation index points into a character array maintained by the containing state structure, allowing efficient storage of timezone abbreviations.