# leapadd

## Location
[src/timezone/zic.c:3403-3424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L3403-L3424)

## Overview
The leapadd function inserts a new leap second entry into the global leap second tables, maintaining chronological order and tracking leap second corrections for accurate timekeeping.

## Definition

```c
static void
leapadd(zic_t t, int correction, int rolling)
```
## Detailed Description
The leapadd function manages leap second data within PostgreSQL's timezone compiler by inserting new leap second entries while preserving chronological order. It validates that the maximum number of leap seconds hasn't been exceeded, finds the correct insertion point based on the timestamp, and shifts existing entries to accommodate the new leap second. The function maintains three parallel arrays tracking transition times, correction values, and rolling indicators, ensuring data consistency across all leap second information.

## Parameters / Member Variables
- : A zic_t timestamp indicating when the leap second occurs
- : An integer representing the cumulative leap second correction value at this time
- : An integer flag indicating whether this leap second uses rolling time semantics

## Dependencies
- Functions called/Symbols referenced:
  - TZ_MAX_LEAPS (maximum leap second limit constant)
  - EXIT_FAILURE (error exit status)
  - zic_t (timestamp type definition)
- Called from (representative examples):
  - [inleap](../i/inleap.md) (at line 1780)

## Notes and Other Information
- Maintains chronological order by finding the correct insertion point and shifting subsequent entries
- Uses memmove() for safe array element shifting to avoid data corruption
- Exits with failure if TZ_MAX_LEAPS limit is exceeded
- Updates three global arrays: trans[] (timestamps), corr[] (corrections), roll[] (rolling flags)
- Increments the global leapcnt counter to track total leap second entries
- Essential for accurate time calculations that must account for leap second adjustments

## Simplified Source

```c
static void leapadd(zic_t t, int correction, int rolling) {
    int i;

    // Check maximum leap seconds limit
    if (TZ_MAX_LEAPS <= leapcnt) {
        error(_("too many leap seconds"));
        exit(EXIT_FAILURE);
    }

    // Find insertion point to maintain chronological order
    for (i = 0; i < leapcnt; ++i)
        if (t <= trans[i])
            break;

    // Shift existing entries to make room for new leap second
    memmove(&trans[i + 1], &trans[i], (leapcnt - i) * sizeof *trans);
    memmove(&corr[i + 1], &corr[i], (leapcnt - i) * sizeof *corr);
    memmove(&roll[i + 1], &roll[i], (leapcnt - i) * sizeof *roll);

    // Insert new leap second data
    trans[i] = t;
    corr[i] = correction;
    roll[i] = rolling;
    ++leapcnt;
}
```