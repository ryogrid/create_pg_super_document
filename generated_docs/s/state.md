# state

## Location
[src/timezone/pgtz.h:41-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/pgtz.h#L41-L64)

## Overview
The state struct represents the complete timezone state information, containing all data needed to perform timezone calculations for a specific timezone.

## Definition
```c
struct state
{
    int         leapcnt;
    int         timecnt;
    int         typecnt;
    int         charcnt;
    bool        goback;
    bool        goahead;
    pg_time_t   ats[TZ_MAX_TIMES];
    unsigned char types[TZ_MAX_TIMES];
    struct ttinfo ttis[TZ_MAX_TYPES];
    char        chars[BIGGEST(BIGGEST(TZ_MAX_CHARS + 1, 4 /* sizeof gmt */ ),
                              (2 * (TZ_STRLEN_MAX + 1)))];
    struct lsinfo lsis[TZ_MAX_LEAPS];
    int         defaulttype;
};
```

## Detailed Description
The state struct is the central data structure for timezone handling in PostgreSQL, containing all necessary information to convert between local time and UTC for a specific timezone. It stores transition times, timezone types, leap second information, and abbreviation strings. This structure supports complex timezone rules including historical changes, daylight saving time transitions, and leap second adjustments.

## Parameters / Member Variables
- `leapcnt`: Number of leap second transitions stored
- `timecnt`: Number of time transitions stored  
- `typecnt`: Number of timezone types stored
- `charcnt`: Number of characters used in the abbreviation string buffer
- `goback`: Boolean indicating whether to extrapolate backward beyond stored data
- `goahead`: Boolean indicating whether to extrapolate forward beyond stored data
- `ats[]`: Array of transition times when timezone rules change
- `types[]`: Array mapping each transition to its corresponding timezone type
- `ttis[]`: Array of timezone type information structures
- `chars[]`: Character buffer storing timezone abbreviation strings
- `lsis[]`: Array of leap second information structures
- `defaulttype`: The timezone type to use for times before any transitions or when no transitions exist

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_t (PostgreSQL time type)
  - struct ttinfo (time type information)
  - struct lsinfo (leap second information)
  - TZ_MAX_TIMES, TZ_MAX_TYPES, TZ_MAX_CHARS, TZ_MAX_LEAPS (timezone constants)
  - TZ_STRLEN_MAX (timezone string length constant)
- Called from (representative examples):
  - [tzload](../t/tzload.md) (timezone data loading)
  - [tzparse](../t/tzparse.md) (timezone parsing)
  - [gmtload](../g/gmtload.md) (GMT timezone loading)
  - [localsub](../l/localsub.md) (local time calculations)
  - [timesub](../t/timesub.md) (time calculations)
  - [leapcorr](../l/leapcorr.md) (leap second corrections)

## Notes and Other Information
This structure represents the complete timezone database information for a single timezone. The arrays are sized according to maximum limits defined by the timezone library. The goback and goahead flags control extrapolation behavior when converting times outside the range of stored transition data. The defaulttype field is used for early times or when no transitions are defined, and is typically zero for recent timezone database releases.