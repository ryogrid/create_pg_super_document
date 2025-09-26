# rpytime

## Location
[src/timezone/zic.c:3801-3864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L3801-L3864)

## Overview
Computes the date (in seconds since January 1, 1970, 00:00 LOCAL time) for a given timezone rule in a specific year.

## Definition
```c
static zic_t rpytime(const struct rule *rp, zic_t wantedy)
```

## Detailed Description
The `rpytime` function calculates the exact timestamp when a timezone rule takes effect in a given year. It handles complex date calculations including leap years, day-of-week dependencies, and special calendar cases like February 29th in non-leap years. The function performs several key operations:

1. Handles boundary cases for minimum and maximum representable times
2. Calculates day offset from epoch year through efficient year cycling
3. Advances through months to reach the target month
4. Handles special February 29th cases in non-leap years
5. Processes day-of-week rules (e.g., "last Sunday", "first Monday >= 8th")
6. Validates that computed dates fall within valid month boundaries
7. Converts final day offset to seconds and adds time-of-day component

## Parameters / Member Variables
- `rp`: Pointer to a timezone rule structure containing month, day, time, and day-code information
- `wantedy`: The target year for which to compute the rule timestamp

## Dependencies
- Functions called/Symbols referenced:
  - `isleap`: Check if year is a leap year
  - `[oadd](../o/oadd.md)`: Overflow-safe addition for day calculations
  - `[tadd](../t/tadd.md)`: Overflow-safe addition for time calculations
  - [error](../e/error.md): Error reporting function
  - [warning](../w/warning.md): Warning message function
- Called from (representative examples):
  - `[inzsub](../i/inzsub.md)`: Timezone initialization subprocess
  - [years_of_observations](../y/years_of_observations.md): Year range calculation function

## Notes and Other Information
- Returns `min_time` or `max_time` for boundary year values (ZIC_MIN/ZIC_MAX)
- Handles negative years and implements efficient year cycling using YEARSPERREPEAT
- Includes special handling for February 29th in non-leap years based on rule type
- Validates day-of-week rules to ensure dates remain within month boundaries
- Uses EPOCH_YEAR (1970) as the reference point for all calculations
- The "nod to Margaret O." comment refers to a humorous variable name for day offset