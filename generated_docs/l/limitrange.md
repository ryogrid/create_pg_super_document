# limitrange

## Location
[src/timezone/zic.c:2055-2081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L2055-L2081)

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

## Simplified Source

```c
static struct timerange limitrange(struct timerange r, zic_t lo, zic_t hi,
                                   zic_t const *ats, unsigned char const *types) {

    // Trim transitions that occur before the lower bound
    while (r.count > 0 && ats[r.base] < lo) {
        r.defaulttype = types[r.base];  // Update default type to last trimmed transition
        r.count--;
        r.base++;
    }

    // Trim leap seconds that occur before the lower bound
    while (r.leapcount > 0 && trans[r.leapbase] < lo) {
        r.leapcount--;
        r.leapbase++;
    }

    // Trim transitions and leap seconds that occur after the upper bound
    // (only if hi is not the maximum value to avoid overflow)
    if (hi < ZIC_MAX) {
        while (r.count > 0 && hi + 1 < ats[r.base + r.count - 1]) {
            r.count--;
        }
        while (r.leapcount > 0 && hi + 1 < trans[r.leapbase + r.leapcount - 1]) {
            r.leapcount--;
        }
    }

    return r;
}
```

**Key simplifications:**
- Added descriptive comments explaining each trimming phase
- Clarified the logic for updating the default timezone type
- Explained the overflow protection with ZIC_MAX check
- Made the structure of the function more readable with spacing
- Preserved the essential range-limiting algorithm