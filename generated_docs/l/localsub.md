# localsub

## Location
[src/timezone/localtime.c:1259-1343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L1259-L1343)

## Overview
Converts a UTC timestamp to local time using timezone state information, implementing the core logic for local time conversion with support for historical and future timezone transitions.

## Definition

```c
static struct pg_tm *
localsub(struct state const *sp, pg_time_t const *timep,
		 struct pg_tm *const tmp)
```
## Detailed Description
The  function is the core implementation for converting UTC timestamps to local time. It takes a timezone state structure containing transition rules and a UTC timestamp, then calculates the corresponding local time representation. 

The function handles several complex scenarios:
1. **Null timezone state**: Falls back to GMT conversion using 
2. **Historical/Future extrapolation**: When the timestamp falls outside the available transition data range, it uses a sophisticated algorithm to extrapolate based on repeating year patterns
3. **Binary search optimization**: For timestamps within the transition data range, it performs a binary search to efficiently find the appropriate timezone rule
4. **Recursive handling**: For out-of-range timestamps, it recursively calls itself with adjusted timestamps to leverage existing transition data

The function implements a "drop-in replacement" design that avoids calling the standard library's  function while providing equivalent functionality.

## Parameters / Member Variables
- `*sp`: Pointer to a  containing timezone transition data, rules, and abbreviations. If NULL, defaults to GMT conversion.
- `*timep`: Pointer to a  value representing the UTC timestamp to convert.
- `tmp`: Pointer to a  structure that will be populated with the converted local time values.
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

## Simplified Source

```c
static struct pg_tm *
localsub(struct state const *sp, pg_time_t const *timep,
         struct pg_tm *const tmp)
{
    const pg_time_t t = *timep;

    // Handle null timezone state - fallback to GMT
    if (sp == NULL)
        return gmtsub(timep, 0, tmp);

    // Handle out-of-range timestamps using year extrapolation
    if ((sp->goback && t < sp->ats[0]) ||
        (sp->goahead && t > sp->ats[sp->timecnt - 1]))
    {
        // Calculate adjustment based on repeating year cycles
        pg_time_t newt = t;
        pg_time_t seconds = (t < sp->ats[0]) ?
            sp->ats[0] - t : t - sp->ats[sp->timecnt - 1];
        pg_time_t years = (seconds / SECSPERREPEAT + 1) * YEARSPERREPEAT;

        // Adjust timestamp to fall within valid range
        if (t < sp->ats[0])
            newt += years * AVGSECSPERYEAR;
        else
            newt -= years * AVGSECSPERYEAR;

        // Recursively process adjusted timestamp
        struct pg_tm *result = localsub(sp, &newt, tmp);
        if (result) {
            // Apply year adjustment to result
            int64 newy = result->tm_year + (t < sp->ats[0] ? -years : years);
            if (INT_MIN <= newy && newy <= INT_MAX)
                result->tm_year = newy;
            else
                return NULL;
        }
        return result;
    }

    // Find appropriate timezone type using binary search
    int timezone_type;
    if (sp->timecnt == 0 || t < sp->ats[0]) {
        timezone_type = sp->defaulttype;
    } else {
        // Binary search for transition time
        int lo = 1, hi = sp->timecnt;
        while (lo < hi) {
            int mid = (lo + hi) >> 1;
            if (t < sp->ats[mid])
                hi = mid;
            else
                lo = mid + 1;
        }
        timezone_type = sp->types[lo - 1];
    }

    // Get timezone info and convert timestamp
    const struct ttinfo *ttisp = &sp->ttis[timezone_type];
    struct pg_tm *result = timesub(&t, ttisp->tt_utoff, sp, tmp);

    if (result) {
        result->tm_isdst = ttisp->tt_isdst;
        result->tm_zone = &sp->chars[ttisp->tt_desigidx];
    }

    return result;
}
```