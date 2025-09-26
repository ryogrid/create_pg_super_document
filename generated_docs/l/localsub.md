# localsub

## Location
src/timezone/localtime.c: 1259 - 1343

## Overview
Converts a UTC timestamp to local time using timezone state information, implementing the core logic for local time conversion with support for historical and future timezone transitions.

## Definition


## Detailed Description
The  function is the core implementation for converting UTC timestamps to local time. It takes a timezone state structure containing transition rules and a UTC timestamp, then calculates the corresponding local time representation. 

The function handles several complex scenarios:
1. **Null timezone state**: Falls back to GMT conversion using 
2. **Historical/Future extrapolation**: When the timestamp falls outside the available transition data range, it uses a sophisticated algorithm to extrapolate based on repeating year patterns
3. **Binary search optimization**: For timestamps within the transition data range, it performs a binary search to efficiently find the appropriate timezone rule
4. **Recursive handling**: For out-of-range timestamps, it recursively calls itself with adjusted timestamps to leverage existing transition data

The function implements a "drop-in replacement" design that avoids calling the standard library's  function while providing equivalent functionality.

## Parameters / Member Variables
- : Pointer to a  containing timezone transition data, rules, and abbreviations. If NULL, defaults to GMT conversion.
- : Pointer to a  value representing the UTC timestamp to convert.
- : Pointer to a  structure that will be populated with the converted local time values.

## Dependencies
- Functions called/Symbols referenced:
  -  (fallback for GMT conversion when sp is NULL)
  -  (performs the actual time structure calculation)
  -  (utility for const casting)
  - Constants: , , 
- Called from (representative examples):
  -  (recursive call for out-of-range timestamps)
  -  (in src/timezone/localtime.c:1346)

## Notes and Other Information
- This is a static function, only accessible within the localtime.c file
- The function includes a sophisticated year extrapolation algorithm for timestamps outside the transition data range, using repeating patterns based on calendar cycles
- Binary search is used for efficient lookup of transition rules within the valid range
- The function handles both historical (backwards) and future (forwards) time extrapolation using the  and  flags in the state structure
- Includes compatibility notes referencing System V Release 2.0 behavior differences
- The recursive approach for out-of-range timestamps ensures consistent behavior across the entire supported time range
- Critical component of PostgreSQL's timezone conversion system, enabling accurate local time calculations for database timestamp operations