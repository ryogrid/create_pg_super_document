# stringoffset

## Location
src/timezone/zic.c: 2682 - 2715

## Overview
The stringoffset function converts a timezone offset value (in seconds) into a human-readable string format suitable for POSIX timezone strings and display purposes.

## Definition


## Detailed Description
This function formats timezone offsets into a string representation following the format H[:MM[:SS]], where:
- H represents hours (can be more than 24 for extreme cases)
- MM represents minutes (00-59) 
- SS represents seconds (00-59)

The function automatically handles negative offsets by prepending a minus sign. It also optimizes the output by omitting minutes and seconds when they are zero (e.g., "5" instead of "5:00:00" for a 5-hour offset).

The function includes validation to prevent extremely large offsets (>= HOURSPERDAY * DAYSPERWEEK hours) that would be unrealistic for timezone purposes, returning an empty string in such cases.

## Parameters / Member Variables
- : Output buffer where the formatted offset string will be written
- : The timezone offset in seconds (positive or negative)

## Dependencies
- Functions called/Symbols referenced:
  - zic_t (timezone calculation type)
  - SECSPERMIN (constant for seconds per minute)
  - MINSPERHOUR (constant for minutes per hour) 
  - HOURSPERDAY (constant for hours per day)
  - DAYSPERWEEK (constant for days per week)
  - sprintf (for formatted string output)
- Called from (representative examples):
  - stringrule (in src/timezone/zic.c:2780)
  - stringzone (in src/timezone/zic.c:2901, 2914)

## Notes and Other Information
- Returns the length of the generated string
- Automatically handles sign conversion for negative offsets
- Provides compact output by omitting zero minutes/seconds
- Used in POSIX timezone string generation where offsets need to be human-readable
- Validates against unreasonably large offsets and returns empty string for invalid values
- Essential component in timezone rule string formatting and display