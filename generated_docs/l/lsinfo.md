# lsinfo

## Location
[src/timezone/pgtz.h:35-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/pgtz.h#L35-L40)

## Overview
The lsinfo struct represents leap second information, storing the transition time and correction value for leap second adjustments in timezone calculations.

## Definition
```c
struct lsinfo
{                               /* leap second information */
    pg_time_t   ls_trans;       /* transition time */
    int64       ls_corr;        /* correction to apply */
};
```

## Detailed Description
The lsinfo struct defines leap second transition information used by PostgreSQL's timezone system to handle leap seconds correctly. Leap seconds are occasional one-second adjustments made to Coordinated Universal Time (UTC) to account for irregularities in Earth's rotation. This structure stores when a leap second occurs and what correction should be applied to maintain accurate timekeeping.

## Parameters / Member Variables
- `ls_trans`: The transition time when the leap second occurs, stored as a pg_time_t value
- `ls_corr`: The correction value to apply at the transition time, typically +1 or -1 second

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_t (PostgreSQL time type)
- Called from (representative examples):
  - timesub (time subtraction calculations)
  - leapcorr (leap second correction function)
  - [state](../s/state.md) struct (as member lsis array)

## Notes and Other Information
Leap seconds are relatively rare events, typically occurring at most twice per year. PostgreSQL's timezone library maintains an array of these structures to track all historical and future leap seconds. The correction value is usually +1 second (positive leap second) but could theoretically be -1 second (negative leap second), though no negative leap seconds have been implemented as of recent timezone databases.