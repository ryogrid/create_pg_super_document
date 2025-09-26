# leaps_thru_end_of

## Location
src/timezone/localtime.c: 1406 - 1413

## Overview
Calculates the number of leap years from year 0 through the end of any given year, supporting both positive and negative years through delegation to specialized helper functions.

## Definition


## Detailed Description
The  function serves as a comprehensive wrapper for leap year calculations that handles the full range of possible year values, including negative years. It implements a branching strategy:

1. **Negative years**: Uses a mathematical transformation to convert negative year calculations into positive year calculations, then applies the appropriate adjustment
2. **Non-negative years**: Directly delegates to  for efficient calculation

For negative years, the function uses the mathematical identity that the leap year count for negative year  is the negative of the leap year count for positive year , minus 1. This transformation:  correctly handles the symmetry of leap year calculations across the year 0 boundary.

The function ensures consistent leap year counting across the entire range of representable years, which is essential for accurate date and time calculations in PostgreSQL's timezone system.

## Parameters / Member Variables
- : An integer representing the year for which to calculate the cumulative leap year count. Can be any representable integer value (positive, negative, or zero).

## Dependencies
- Functions called/Symbols referenced:
  -  (called twice - once for negative year transformation, once for direct positive year calculation)
- Called from (representative examples):
  -  (multiple calls in src/timezone/localtime.c:1461, 1462, 1506, 1507)

## Notes and Other Information
- This is a static function, only accessible within the localtime.c file
- Provides a unified interface for leap year calculations regardless of whether the year is positive or negative
- The mathematical transformation for negative years ensures that leap year calculations are consistent and symmetric around year 0
- Critical for accurate date arithmetic in PostgreSQL's timezone and timestamp systems
- The function handles edge cases around year 0 and provides consistent results for historical date calculations
- Used extensively by the  function, which is core to PostgreSQL's time conversion functionality
- The design separates the complexity of negative year handling from the optimized positive year calculation
- Ensures mathematical correctness for date calculations spanning from prehistoric to future time periods