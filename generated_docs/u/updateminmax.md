# updateminmax

## Location
src/timezone/zic.c: 2673 - 2681

## Overview
The updateminmax function maintains global minimum and maximum year values by updating them when a new year value is encountered.

## Definition


## Detailed Description
This is a simple utility function that tracks the range of years being processed in the timezone compiler. It compares the input value against the current global minimum and maximum year values, updating them if the new value extends the range in either direction.

The function serves as a bounds tracker for timezone data processing, ensuring that the compiler knows the full temporal range of the timezone rules being processed. This information is used later for optimization and validation purposes.

## Parameters / Member Variables
- : The year value to compare against current minimum and maximum bounds

## Dependencies
- Functions called/Symbols referenced:
  - zic_t (timezone calculation type)
  - min_year (global variable for tracking minimum year)
  - max_year (global variable for tracking maximum year)
- Called from (representative examples):
  - outzone (in src/timezone/zic.c:3003, 3004, 3010, 3015, 3017)

## Notes and Other Information
- Uses global variables min_year and max_year to maintain state across calls
- Essential for timezone data validation and range checking
- Simple but critical function for tracking the temporal scope of timezone rules
- Called frequently during timezone rule processing to maintain accurate bounds
- The tracked range helps optimize memory allocation and processing decisions later in the compilation process