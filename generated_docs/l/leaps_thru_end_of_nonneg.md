# leaps_thru_end_of_nonneg

## Location
[src/timezone/localtime.c:1400-1405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L1400-L1405)

## Overview
Calculates the number of leap years that occur from year 0 through the end of a given non-negative year, using the Gregorian calendar leap year rules.

## Definition

```c
static int
leaps_thru_end_of_nonneg(int y)
```
## Detailed Description
The  function implements the mathematical calculation for determining leap years in the Gregorian calendar system. It applies the standard leap year rules:
- Years divisible by 4 are leap years
- Years divisible by 100 are NOT leap years (exception to the first rule)  
- Years divisible by 400 ARE leap years (exception to the second rule)

The function uses integer division to efficiently count leap years without iteration. The calculation  works because:
1.  counts all years divisible by 4
2.  subtracts years divisible by 100 (which shouldn't be leap years)
3.  adds back years divisible by 400 (which should be leap years)

The function is specifically designed for non-negative years and defines year 0 as having zero leap years for mathematical convenience.

## Parameters / Member Variables
- `y`: An integer representing the year for which to calculate the cumulative leap year count. Must be non-negative (>= 0).
## Dependencies
- Functions called/Symbols referenced:
  - None (performs only arithmetic operations)
- Called from (representative examples):
  -  (twice in src/timezone/localtime.c:1409-1410)

## Notes and Other Information
- This is a static function, only accessible within the localtime.c file
- The function is optimized for non-negative years only; negative year handling is delegated to the caller
- Uses integer division which automatically truncates, providing the correct count of complete leap year cycles
- The mathematical approach avoids loops and provides O(1) complexity
- Year 0 is explicitly defined to return 0 leap years, which simplifies calculations in the broader timezone system
- Critical component for accurate date calculations in PostgreSQL's timezone and timestamp handling
- The Gregorian calendar rules implemented here are essential for correct historical and future date computations
- Part of PostgreSQL's timezone infrastructure that ensures accurate leap year handling across different calendar calculations