# addtt

## Location
[src/timezone/zic.c:3348-3357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L3348-L3357)

## Overview
The addtt function adds a new time transition entry to the global attypes array, recording a timezone transition at a specific time with its associated type.

## Definition


## Detailed Description
The addtt function is a utility function within the PostgreSQL timezone compiler (zic) that manages time transition records. It dynamically grows the attypes array to accommodate new entries and stores timezone transition information including the timestamp when the transition occurs and the timezone type that becomes active at that time. The function automatically increments the global timecnt counter to track the total number of transitions.

## Parameters / Member Variables
- : A zic_t timestamp indicating when this timezone transition takes effect
- : An integer identifier representing the timezone type (offset, DST rules, etc.) that becomes active at the specified time

## Dependencies
- Functions called/Symbols referenced:
  - growalloc (for dynamic array management)
  - zic_t (timestamp type definition)
- Called from (representative examples):
  - [years_of_observations](../y/years_of_observations.md) (multiple calls at lines 3123, 3269, 3291, 3336)

## Notes and Other Information
- This is a static function local to the zic.c file, used internally by the timezone compiler
- The function sets dontmerge to false by default, allowing potential optimization of adjacent identical transitions
- Uses the global variables attypes, timecnt, and timecnt_alloc for state management
- Part of PostgreSQL's timezone data compilation infrastructure