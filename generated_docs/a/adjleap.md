# adjleap

## Location
[src/timezone/zic.c:3425-3467](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L3425-L3467)

## Overview
The adjleap function processes and validates leap second data by propagating leap second corrections forward through time and handling leap second expiration settings.

## Definition

```c
static void
adjleap(void)
```
## Detailed Description
The adjleap function performs post-processing of leap second data within PostgreSQL's timezone compiler. It propagates cumulative leap second corrections forward through all leap second entries, ensuring proper temporal relationships and validating minimum spacing between leap seconds. The function also handles leap second expiration times, checking for consistency between the last leap second and any specified expiration date. Additionally, it adjusts the global time boundaries based on leap second expiration constraints to maintain temporal integrity.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [tadd](../t/tadd.md) (time addition with overflow handling)
  - [oadd](../o/oadd.md) (overflow-safe addition)
  - [warning](../w/warning.md) (warning message output)
  - SECSPERDAY (seconds per day constant)
  - EXIT_FAILURE (error exit status)
  - zic_t (timestamp type definition)
- Called from (representative examples):
  - [main](../m/main.md) (at line 811)

## Notes and Other Information
- Validates that leap seconds are spaced at least 28 days apart to comply with leap second regulations
- Propagates cumulative leap second corrections through the trans[] and corr[] arrays
- Handles backward compatibility for obsolescent "#expires" syntax with warning
- Ensures leap second expiration time follows the last leap second transition
- Adjusts the global hi_time boundary when leap second expiration constrains the valid time range
- Uses overflow-safe arithmetic functions (tadd, oadd) for robust time calculations
- Critical for maintaining leap second data integrity and temporal consistency in timezone files

## Simplified Source

```c
static void adjleap(void) {
    int i;
    zic_t last = 0;
    zic_t prevtrans = 0;

    // Propagate leap seconds forward, validate spacing
    for (i = 0; i < leapcnt; ++i) {
        if (trans[i] - prevtrans < 28 * SECSPERDAY) {
            error(_("Leap seconds too close together"));
            exit(EXIT_FAILURE);
        }
        prevtrans = trans[i];
        trans[i] = tadd(trans[i], last);    // Add cumulative correction
        last = corr[i] += last;             // Update correction total
    }

    // Handle leap expiration settings
    if (leapexpires < 0) {
        leapexpires = comment_leapexpires;
        if (0 <= leapexpires)
            warning(_("\"#expires\" is obsolescent; use \"Expires\""));
    }

    // Validate expiration time consistency
    if (0 <= leapexpires) {
        leapexpires = oadd(leapexpires, last);
        if (!(leapcnt == 0 || (trans[leapcnt - 1] < leapexpires))) {
            error(_("last Leap time does not precede Expires time"));
            exit(EXIT_FAILURE);
        }
        if (leapexpires <= hi_time)
            hi_time = leapexpires - 1;      // Adjust time boundary
    }
}
```