# compare_tm

## Location
[src/bin/initdb/findtimezone.c:207-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/findtimezone.c#L207-L233)

## Overview
Compares a system tm structure with a PostgreSQL-specific pg_tm structure to determine if they represent the same time values.

## Definition

```c
struct tm *s, struct pg_tm *p)
{
	if (s->tm_sec != p->tm_sec ||
		s->tm_min != p->tm_min ||
		s->tm_hour != p->tm_hour ||
		s->tm_mday != p->tm_mday ||
		s->tm_mon != p->tm_mon ||
		s->tm_year != p->tm_year ||
		s->tm_wday != p->tm_wday ||
		s->tm_yday != p->tm_yday ||
		s->tm_isdst != p->tm_isdst)
		return false;
	return true;
}

/*
 * See how well a specific timezone setting matches the system behavior
 *
 * We score a timezone setting according to the number of test times it
 * matches.  (The test times are ordered later-to-earlier, but this routine
 * doesn't actually know that;
```
## Detailed Description
This function performs a field-by-field comparison between a standard C library  and PostgreSQL's  to verify if they contain identical time information. It checks all relevant time components including seconds, minutes, hours, day, month, year, day of week, day of year, and daylight saving time flag. The function is used internally during timezone detection and validation processes to ensure that PostgreSQL's time calculations match the system's time calculations.

## Parameters / Member Variables
- `false`: Pointer to a standard C library  containing system time information
- `true`: Pointer to a PostgreSQL-specific  containing PostgreSQL's calculated time information

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tm](../p/pg_tm.md) (PostgreSQL time structure type)
- Called from (representative examples):
  - [score_timezone](../s/score_timezone.md)

## Notes and Other Information
- This function is marked as , indicating it's only used within the findtimezone.c file
- Returns  if all time fields match exactly,  if any field differs
- Part of the timezone detection mechanism in initdb
- Used to validate that PostgreSQL's timezone calculations align with the system's timezone behavior

## Simplified Source

```c
static bool
compare_tm(struct tm *s, struct pg_tm *p)
{
    // Compare all time fields between system and PostgreSQL time structures
    if (s->tm_sec != p->tm_sec ||
        s->tm_min != p->tm_min ||
        s->tm_hour != p->tm_hour ||
        s->tm_mday != p->tm_mday ||
        s->tm_mon != p->tm_mon ||
        s->tm_year != p->tm_year ||
        s->tm_wday != p->tm_wday ||
        s->tm_yday != p->tm_yday ||
        s->tm_isdst != p->tm_isdst)
        return false;

    return true;
}
```