# years_of_observations

## Location
[src/timezone/zic.c:3062-3347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L3062-L3347)

## Overview
An enumerated constant that defines the number of years of timezone data to generate when extending beyond explicit rules, calculated as the POSIX repetition cycle plus a safety margin.

## Definition
```c
enum
{
    years_of_observations = YEARSPERREPEAT + 2
};
```

## Detailed Description
The `years_of_observations` constant is used within the `outzone` function to determine how many years of timezone data should be generated when a timezone cannot be fully represented by a POSIX timezone string. It represents the number of years of historical timezone behavior that should be observed and repeated for future predictions.

The value is calculated as `YEARSPERREPEAT + 2`, where:
- `YEARSPERREPEAT` is typically 400 (the Gregorian calendar cycle)
- The additional 2 years provide a safety margin to avoid edge cases

This constant helps handle scenarios where timezone rules have complex patterns that extend beyond what can be expressed in a simple POSIX timezone string, ensuring that enough historical data is captured to make accurate future predictions.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - `YEARSPERREPEAT` (constant defining the calendar repetition cycle)
- Called from (representative examples):
  - Used in year range calculations within `outzone` function
  - Referenced in min_year and max_year boundary calculations

## Notes and Other Information
- Defined as a local enumerated constant within the `outzone` function scope
- The 2-year safety margin helps avoid edge cases in timezone rule transitions
- Used to extend timezone data generation beyond explicit rule definitions
- Critical for handling non-POSIX timezone rules that require historical pattern analysis
- Part of PostgreSQL's strategy for generating comprehensive timezone data files
- Ensures compatibility with timezones that have complex historical daylight saving patterns