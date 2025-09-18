# limitrange

## Location
src/timezone/zic.c: 2055 - 2081

## Overview
A static function that adjusts a timerange structure to fit within specified time bounds by trimming transitions and leap seconds outside the given range.

## Definition
```c
static struct timerange limitrange(struct timerange r, zic_t lo, zic_t hi,
                                   zic_t const *ats, unsigned char const *types)
```

## Detailed Description
The `limitrange` function takes a timerange structure and constrains it to fit within the specified low and high time bounds. It performs two main operations: first, it trims transitions that occur before the low bound (`lo`) by advancing the base index and updating the default timezone type; second, it trims transitions that occur after the high bound (`hi`) by reducing the count of transitions. The function also handles leap second data similarly, ensuring that both transition times and leap seconds fall within the specified range. This is essential for generating timezone files that cover only specific time periods.

## Parameters / Member Variables
- `r`: A timerange structure containing timezone transition data to be limited
- `lo`: Lower bound timestamp (zic_t) - transitions before this time will be trimmed
- `hi`: Upper bound timestamp (zic_t) - transitions after this time will be trimmed  
- `ats`: Array of transition timestamps (zic_t const *) used for boundary checking
- `types`: Array of timezone types (unsigned char const *) corresponding to transitions

## Dependencies
- Functions called/Symbols referenced:
  - [timerange](../t/timerange.md) (structure type for timezone range data)
  - zic_t (type definition for timezone timestamps)
  - ZIC_MAX (maximum time value constant)
  - trans (global array for leap second transitions)
- Called from (representative examples):
  - [writezone](../w/writezone.md) (called twice, likely for different time ranges)

## Notes and Other Information
- The function modifies the base index and counts to effectively `slice` the time range data
- When trimming early transitions, it updates the default timezone type to the last trimmed transition's type
- The function handles both regular timezone transitions and leap second transitions
- Uses the global `trans` array for leap second data management
- Only trims high-end transitions when `hi` is less than ZIC_MAX (avoiding overflow issues)
- Part of PostgreSQL's timezone data compilation infrastructure for time range management
- This function is static and only accessible within the zic.c compilation unit